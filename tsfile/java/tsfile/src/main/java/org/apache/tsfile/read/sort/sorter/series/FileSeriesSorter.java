package org.apache.tsfile.read.sort.sorter.series;


import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.controller.IChunkLoader;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.sort.sorter.chunk.ChunkSorter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Series sorter is used to sort one series of one TsFile, and this reader has a filter operating
 * on the same series.
 */

public class FileSeriesSorter {
    protected IChunkLoader chunkLoader;
    protected List<IChunkMetadata> chunkMetadataList;
    protected ChunkSorter chunkSorter;
    protected List<String> currentChunkMeasurementNames = new ArrayList<>();
    private int chunkToRead;
    protected boolean ignoreAllNullRows;

    protected Filter filter;

    public FileSeriesSorter(
            IChunkLoader chunkLoader, List<IChunkMetadata> chunkMetadataList, Filter filter) {
        this.chunkLoader = chunkLoader;
        this.chunkMetadataList = chunkMetadataList;
    }

    public void initChunkSorter(IChunkMetadata chunkMetaData, String fileName) throws IOException {
        currentChunkMeasurementNames.clear();
        if (chunkMetaData instanceof ChunkMetadata) {
            long offsetOfChunkHeader = chunkMetaData.getOffsetOfChunkHeader();
            Chunk chunk = chunkLoader.loadChunk((ChunkMetadata) chunkMetaData);
            this.chunkSorter = new ChunkSorter(fileName, chunk, offsetOfChunkHeader);
            chunkSorter.sort();
            chunkSorter.flush();
            currentChunkMeasurementNames.add(chunkMetaData.getMeasurementUid());
        } else {
            return;
        }
    }
}
