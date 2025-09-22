package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ICrossCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.IUnseqCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.subtask.ReadPointPerformerSubTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.subtask.ReadPointUndecodePerformerSubtask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.MultiTsFileDeviceIterator;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.reader.IDataBlockReader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.reader.SeriesDataBlockReader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.writer.AbstractCompactionWriter;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.writer.FastInnerCompactionWriter;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.writer.ReadPointCrossCompactionWriter;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.writer.ReadPointInnerCompactionWriter;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.control.QueryResourceManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.encoding.sorter.PersistUncompressingSorter;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.statistics.LongStatistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.utils.CompressedPageData;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.h2.mvstore.Page;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class ReadPointUndecodeCompactionPerformer
        implements ICrossCompactionPerformer, IUnseqCompactionPerformer {
        @SuppressWarnings("squid:S1068")
        private final Logger logger = LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);

        protected List<TsFileResource> seqFiles = Collections.emptyList();
        protected List<TsFileResource> unseqFiles = Collections.emptyList();

        private static final int SUB_TASK_NUM =
                IoTDBDescriptor.getInstance().getConfig().getSubCompactionTaskNum();

        private CompactionTaskSummary summary;

        protected List<TsFileResource> targetFiles = Collections.emptyList();

  public ReadPointUndecodeCompactionPerformer(
                List<TsFileResource> seqFiles,
                List<TsFileResource> unseqFiles,
                List<TsFileResource> targetFiles) {
            this.seqFiles = seqFiles;
            this.unseqFiles = unseqFiles;
            this.targetFiles = targetFiles;
        }

  public ReadPointUndecodeCompactionPerformer(
                List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles) {
            this.seqFiles = seqFiles;
            this.unseqFiles = unseqFiles;
        }

  public ReadPointUndecodeCompactionPerformer() {}

        @SuppressWarnings("squid:S2095") // Do not close device iterator
        @Override
        public void perform() throws Exception {
            long queryId = QueryResourceManager.getInstance().assignCompactionQueryId();
            FragmentInstanceContext fragmentInstanceContext =
                    FragmentInstanceContext.createFragmentInstanceContextForCompaction(queryId);
            QueryDataSource queryDataSource = new QueryDataSource(seqFiles, unseqFiles);
            QueryResourceManager.getInstance()
                    .getQueryFileManager()
                    .addUsedFilesForQuery(queryId, queryDataSource);
            summary.setTemporalFileNum(targetFiles.size());
            try (AbstractCompactionWriter compactionWriter =
                         getCompactionWriter(seqFiles, unseqFiles, targetFiles)) {
                // Do not close device iterator, because tsfile reader is managed by FileReaderManager.
                MultiTsFileDeviceIterator deviceIterator =
                        new MultiTsFileDeviceIterator(seqFiles, unseqFiles);
                while (deviceIterator.hasNextDevice()) {
                    checkThreadInterrupted();
                    Pair<String, Boolean> deviceInfo = deviceIterator.nextDevice();
                    String device = deviceInfo.left;
                    boolean isAligned = deviceInfo.right;
                    queryDataSource.fillOrderIndexes(device, true);

                    if (isAligned) {
                        break;  // aligned timeseries are not supported in undecode compaction
                    } else {
                        compactNonAlignedSeries(
                                device, deviceIterator, compactionWriter, fragmentInstanceContext, queryDataSource);
                    }
                    summary.setTemporalFileSize(compactionWriter.getWriterSize());
                }

                compactionWriter.endFile();
                CompactionUtils.updatePlanIndexes(targetFiles, seqFiles, unseqFiles);

            } finally {
                QueryResourceManager.getInstance().endQuery(queryId);
            }
        }

        @Override
        public void setTargetFiles(List<TsFileResource> targetFiles) {
            this.targetFiles = targetFiles;
        }

        @Override
        public void setSummary(CompactionTaskSummary summary) {
            this.summary = summary;
        }

        private void compactNonAlignedSeries(
                String device,
                MultiTsFileDeviceIterator deviceIterator,
                AbstractCompactionWriter compactionWriter,
                FragmentInstanceContext fragmentInstanceContext,
                QueryDataSource queryDataSource)
      throws IOException, InterruptedException, ExecutionException {
            Map<String, MeasurementSchema> schemaMap = deviceIterator.getAllSchemasOfCurrentDevice();
            List<String> allMeasurements = new ArrayList<>(schemaMap.keySet());
            allMeasurements.sort((String::compareTo));
            int subTaskNums = Math.min(allMeasurements.size(), SUB_TASK_NUM);
            // construct sub tasks and start compacting measurements in parallel
            if (subTaskNums > 0) {
                // assign the measurements for each subtask
                List<String>[] measurementListArray = new List[subTaskNums];
                for (int i = 0, size = allMeasurements.size(); i < size; ++i) {
                    int index = i % subTaskNums;
                    if (measurementListArray[index] == null) {
                        measurementListArray[index] = new LinkedList<>();
                    }
                    measurementListArray[index].add(allMeasurements.get(i));
                }

                compactionWriter.startChunkGroup(device, false);
                List<Future<Void>> futures = new ArrayList<>();
                for (int i = 0; i < subTaskNums; ++i) {
                    futures.add(
                            CompactionTaskManager.getInstance()
                                    .submitSubTask(
                                            new ReadPointUndecodePerformerSubtask(
                                                    device,
                                                    measurementListArray[i],
                                                    fragmentInstanceContext,
                                                    queryDataSource,
                                                    compactionWriter,
                                                    schemaMap,
                                                    i)));
                }
                for (Future<Void> future : futures) {
                    future.get();
                }
                compactionWriter.endChunkGroup();
                // check whether to flush chunk metadata or not
                compactionWriter.checkAndMayFlushChunkMetadata();
            }
        }

        /**
         * Construct series data block reader.
         *
         * @param measurementIds if device is aligned, then measurementIds contain all measurements. If
         *     device is not aligned, then measurementIds only contain one measurement.
         * @throws IllegalPathException if the path is illegal
         */
        public static IDataBlockReader constructReader(
                String deviceId,
                List<String> measurementIds,
                List<IMeasurementSchema> measurementSchemas,
                List<String> allSensors,
                FragmentInstanceContext fragmentInstanceContext,
                QueryDataSource queryDataSource,
        boolean isAlign)
      throws IllegalPathException {
            PartialPath seriesPath;
            if (isAlign) {
                seriesPath = new AlignedPath(deviceId, measurementIds, measurementSchemas);
            } else {
                seriesPath = new MeasurementPath(deviceId, measurementIds.get(0), measurementSchemas.get(0));
            }
            return new SeriesDataBlockReader(
                    seriesPath, new HashSet<>(allSensors), fragmentInstanceContext, queryDataSource, true);
        }

        @SuppressWarnings("squid:S1172")
        public static void writeWithPages(
                AbstractCompactionWriter writer,
                LinkedList<CompressedPageData> pages,
                String device,
        int subTaskId,
        boolean isAligned)
                throws IOException, PageException {
            PersistUncompressingSorter sorter = new PersistUncompressingSorter(pages);
            sorter.sortPage(pages.get(1));
            PageHeader pageHeader = getPageHeader(pages);
            ByteBuffer compressedPageData = getCompressedPageData(pages, pageHeader.getCompressedSize());
            compressedPageData.flip();
            writer.flushNonAlignedPage(compressedPageData, pageHeader, subTaskId);
        }

        protected AbstractCompactionWriter getCompactionWriter(
                List<TsFileResource> seqFileResources,
                List<TsFileResource> unseqFileResources,
                List<TsFileResource> targetFileResources)
      throws IOException {
            if (!seqFileResources.isEmpty() && !unseqFileResources.isEmpty()) {
                // cross space
                return new ReadPointCrossCompactionWriter(targetFileResources, seqFileResources);
            } else {
                // inner space
                return new FastInnerCompactionWriter(targetFileResources.get(0));
            }
        }

        private static ByteBuffer getCompressedPageData(LinkedList<CompressedPageData> pages, int compressedPageSize) throws IOException {
            ByteBuffer compressedPageData = ByteBuffer.allocate(compressedPageSize);
            // 写入时间列压缩数据大小
            int compressedTimeSize = 0;
            for (CompressedPageData pageData : pages) {
                compressedTimeSize += pageData.getPageTimeLen() + (pageData.getCount()+3)/4;
            }
            ReadWriteForEncodingUtils.writeUnsignedVarInt(compressedTimeSize+4, compressedPageData);
            // 写入时间列压缩数据
            int count = 0;
            for( CompressedPageData pageData : pages) {
                compressedPageData.put(pageData.getTimeData().getVals());
                count+= pageData.getCount();
            }
            for(CompressedPageData pageData : pages) {
                compressedPageData.put(pageData.getTimeData().getLens());
            }
            compressedPageData.put((byte) (count));
            compressedPageData.put((byte) (count >> 8));
            compressedPageData.put((byte) (count >> 16));
            compressedPageData.put((byte) (count >> 24));
            // 写入数值列压缩数据
            count = 0;
            for( CompressedPageData pageData : pages) {
                compressedPageData.put(pageData.getValueData().getVals());
                count+= pageData.getCount();
            }
            for(CompressedPageData pageData : pages) {
                compressedPageData.put(pageData.getValueData().getLens());
            }
            compressedPageData.put((byte) (count));
            compressedPageData.put((byte) (count >> 8));
            compressedPageData.put((byte) (count >> 16));
            compressedPageData.put((byte) (count >> 24));

            return compressedPageData;
        }

        private static PageHeader getPageHeader(LinkedList<CompressedPageData> pages) {
            Statistics<? extends Serializable> statistics = new LongStatistics();
            int count = 0;
            int len = 0;
            int compressedTimeSize = 0;
            for (CompressedPageData pageData : pages) {
                count += pageData.getCount();
                len += pageData.getPageTimeLen() + pageData.getPageValueLen() + (pageData.getCount()+3)/4 + (pageData.getCount()+3)/4;
                compressedTimeSize += pageData.getPageTimeLen() + (pageData.getCount()+3)/4;
            }
            statistics.setCount(count);
            statistics.setStartTime(pages.getFirst().getMinTime());
            statistics.setEndTime(pages.getLast().getMaxTime());
            statistics.setEmpty(false);
            int timeLen = computeUnsignedVarIntSize(compressedTimeSize+4);
            return new PageHeader(len+timeLen+8, len+timeLen+8, statistics);
        }

    public static int computeUnsignedVarIntSize(int value) {
        int bytes = 1;
        while ((value & 0xFFFFFF80) != 0L) {
            value >>>= 7;
            bytes++;
        }
        return bytes;
    }

        private void checkThreadInterrupted() throws InterruptedException {
            if (Thread.interrupted() || summary.isCancel()) {
                throw new InterruptedException(
                        String.format(
                                "[Compaction] compaction for target file %s abort", targetFiles.toString()));
            }
        }

        @Override
        public void setSourceFiles(List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles) {
            this.seqFiles = seqFiles;
            this.unseqFiles = unseqFiles;
        }

        @Override
        public void setSourceFiles(List<TsFileResource> unseqFiles) {
            this.unseqFiles = unseqFiles;
        }
}
