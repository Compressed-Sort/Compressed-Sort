package org.apache.tsfile.read;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.NoMeasurementException;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.controller.CachedChunkLoaderImpl;
import org.apache.tsfile.read.controller.IChunkLoader;
import org.apache.tsfile.read.controller.IMetadataQuerier;
import org.apache.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.sort.sorter.series.FileSeriesSorter;
import org.apache.tsfile.utils.BloomFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TsFileSorter {
    private TsFileSequenceReader fileReader;
    private IMetadataQuerier metadataQuerier;
    private IChunkLoader chunkLoader;

    public TsFileSorter(TsFileSequenceReader fileReader) throws IOException {
        this.fileReader = fileReader;
        this.metadataQuerier = new MetadataQuerierByFileImpl(fileReader);
        this.chunkLoader = new CachedChunkLoaderImpl(fileReader);
    }

    public void sort(QueryExpression queryExpression) throws IOException {
        // bloom filter
        BloomFilter bloomFilter = metadataQuerier.getWholeFileMetadata().getBloomFilter();
        List<Path> filteredSeriesPath = new ArrayList<>();
        if (bloomFilter != null) {
            for (Path path : queryExpression.getSelectedSeries()) {
                if (bloomFilter.contains(path.getFullPath())) {
                    filteredSeriesPath.add(path);
                }
            }
            queryExpression.setSelectSeries(filteredSeriesPath);
        }

        metadataQuerier.loadChunkMetaDatas(queryExpression.getSelectedSeries());
        if (queryExpression.hasQueryFilter()) {

        } else {
            try {
                sort(queryExpression.getSelectedSeries());
            } catch (NoMeasurementException e) {
                throw new IOException(e);
            }
        }
        return;
    }

    private void sort(List<Path> selectedPathList)
            throws IOException, NoMeasurementException {
        List<FileSeriesSorter> readersOfSelectedSeries = new ArrayList<>();
        List<TSDataType> dataTypes = new ArrayList<>();

        for (Path path : selectedPathList) {
            List<IChunkMetadata> chunkMetadataList = metadataQuerier.getChunkMetaDataList(path);
            FileSeriesSorter seriesSorter;
            seriesSorter = new FileSeriesSorter(chunkLoader, chunkMetadataList, null);
            seriesSorter.initChunkSorter(chunkMetadataList.get(0), fileReader.getFileName());

        }
        //sort(selectedPathList, dataTypes, readersOfSelectedSeries);
    }


}
