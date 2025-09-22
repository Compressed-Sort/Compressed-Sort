package org.apache.iotdb.db.storageengine.dataregion.compaction.inner;

import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ICompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.ReadPointUndecodeCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class InnerUnseqCompactionWithUndecodePerformerTest {
    static String inputFile1 = "D:\\senior\\DQ\\research\\compressed_sort_paper\\code\\vldb25\\tsfile_segment_data\\10001-10001-0-0.tsfile";
    static String inputFile2 = "D:\\senior\\DQ\\research\\compressed_sort_paper\\code\\vldb25\\tsfile_segment_data\\10002-10002-0-0.tsfile";
    static TsFileResource inputResource1;
    static TsFileResource inputResource2;
    static List<Long> originalTimes = new ArrayList<>();
    static List<Long> originalValues = new ArrayList<>();
    static List<Long> mergedTimes = new ArrayList<>();
    static List<Long> mergedValues = new ArrayList<>();
    List<TsFileResource> toMergeResources = new ArrayList<>();
    // 用来测试无解压合并的方法
    static final String COMPACTION_TEST_SG = "root.compactionTest";
    @Before
    public void setup() throws IOException {
        inputResource1 = TsFilegenerator.generateFile(0,100, 100, inputFile1);
        inputResource2 = TsFilegenerator.generateFile(100,100, 100, inputFile2);
        Thread.currentThread().setName("pool-1-IoTDB-Compaction-Worker-1");
        EnvironmentUtils.envSetUp();
    }

    @After
    public void after() throws IOException {
    }

    @Test
    public void sortOneMeasurementWithoutFilterTest() throws Exception {
        inputResource1.setSeq(false);
        inputResource1.setStatusForTest(TsFileResourceStatus.NORMAL);
        inputResource1.updatePlanIndexes((long) 100);

        inputResource2.setSeq(false);
        inputResource2.setStatusForTest(TsFileResourceStatus.NORMAL);
        inputResource2.updatePlanIndexes((long) 100);
        toMergeResources.add(inputResource1);
        toMergeResources.add(inputResource2);
        TsFileResource targetTsFileResource = TsFileNameGenerator.getInnerCompactionTargetFileResource(toMergeResources, false);

        ICompactionPerformer performer =
                new ReadPointUndecodeCompactionPerformer(
                        Collections.emptyList(),
                        toMergeResources,
                        Collections.singletonList(targetTsFileResource));
        performer.setSummary(new CompactionTaskSummary());
        performer.perform();
        CompactionUtils.moveTargetFile(
                Collections.singletonList(targetTsFileResource), true, COMPACTION_TEST_SG);
        CompactionUtils.combineModsInInnerCompaction(toMergeResources, targetTsFileResource);
        int count1 = getDataFromTsFile(inputResource1, originalTimes, originalValues);
        System.out.println("TsFile1总计: " + count1 + " 条数据");
        int count2 = getDataFromTsFile(inputResource2, originalTimes, originalValues);
        System.out.println("TsFile2总计: " + count2 + " 条数据");
        sortTimeWithValue(originalTimes, originalValues);
        int countTotal = getDataFromTsFile(targetTsFileResource, mergedTimes, mergedValues);
        System.out.println("合并后总计: " + countTotal + " 条数据");
        for(int i = 0; i < originalTimes.size(); i++) {
            boolean equal = originalTimes.get(i).equals(mergedTimes.get(i))
                    && originalValues.get(i).equals(mergedValues.get(i));
            assert equal : "数据不一致, 出错位置: " + i
                    + "\n原始数据: " + originalTimes.get(i) + " , " + originalValues.get(i)
                    + "\n合并数据: " + mergedTimes.get(i) + " , " + mergedValues.get(i);
        }
    }

    private int getDataFromTsFile(TsFileResource tsFileResource, List<Long> times, List<Long> values) {
        // 测试合并后输出的数据是否和输入的一致
        int count = 0;
        try (TsFileSequenceReader reader = new TsFileSequenceReader(tsFileResource.getTsFilePath())) {
            // 获取所有时间序列元数据
            Map<String, List<TimeseriesMetadata>> tsMetadataMap = reader.getAllTimeseriesMetadata(true);

            for (Map.Entry<String, List<TimeseriesMetadata>> entry : tsMetadataMap.entrySet()) {
                String device = entry.getKey();
                List<TimeseriesMetadata> tsMetaList = entry.getValue();

                for (TimeseriesMetadata tsMeta : tsMetaList) {
                    String measurement = tsMeta.getMeasurementId();
                    TSDataType dataType = tsMeta.getTsDataType();

                    // 构造 Path (device + measurement)
                    Path path = new Path(device, measurement, true);

                    // 获取 chunk 元数据
                    List<ChunkMetadata> chunkMetadataList = reader.getChunkMetadataList(path);

                    for (ChunkMetadata chunkMeta : chunkMetadataList) {
                        Chunk chunk = reader.readMemChunk(chunkMeta);
                        ChunkReader chunkReader = new ChunkReader(chunk);

                        while (chunkReader.hasNextSatisfiedPage()) {
                            BatchData batchData = chunkReader.nextPageData();
                            while (batchData.hasCurrent()) {
//                                System.out.println(
//                                        path + " -> 时间戳: " + batchData.currentTime() + " 值: " + batchData.currentValue()
//                                );
                                times.add(batchData.currentTime());
                                values.add(((Long) batchData.currentValue()));
                                batchData.next();
                                count++;
                            }
                        }
                    }
                }
            }
            return count;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void sortTimeWithValue(List<Long> times, List<Long> values) {
        if (times.size() != values.size()) {
            throw new IllegalArgumentException("times 和 values 的长度必须一致");
        }

        // 把时间和值打包到一个列表里
        List<long[]> pairs = new ArrayList<>();
        for (int i = 0; i < times.size(); i++) {
            pairs.add(new long[]{times.get(i), values.get(i)});
        }

        // 按时间排序
        pairs.sort(Comparator.comparingLong(a -> a[0]));

        // 拆回去写入原来的 List
        for (int i = 0; i < pairs.size(); i++) {
            times.set(i, pairs.get(i)[0]);
            values.set(i, pairs.get(i)[1]);
        }
    }
}
