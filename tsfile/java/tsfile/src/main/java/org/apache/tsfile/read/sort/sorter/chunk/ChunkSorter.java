package org.apache.tsfile.read.sort.sorter.chunk;

import org.apache.tsfile.compress.IUnCompressor;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.reader.page.LazyLoadPageData;
import org.apache.tsfile.read.sort.sorter.page.PageSorter;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

public class ChunkSorter {
    long startOffset;
    String fileName = null;
    private final ChunkHeader chunkHeader;
    private final ByteBuffer chunkDataBuffer;
    protected final List<PageSorter> pageSorterList = new LinkedList<>();

    public ChunkSorter(String fileName, Chunk chunk, long chunkOffset) throws IOException {
        this.fileName = fileName;
        this.chunkHeader = chunk.getHeader();
        this.chunkDataBuffer = chunk.getData();
        this.startOffset = chunkOffset+chunkHeader.getSerializedSize();
        initAllPageSorters(chunk.getChunkStatistic());
    }

    public void sort() throws IOException {
        // 分page，对page内的数据排序
        for(PageSorter sorter : pageSorterList) {
            sorter.sort();
        }
    }

    public void flush() throws IOException {
        // 分page，将page中的数据刷写持久化存储
        for(PageSorter sorter : pageSorterList) {
            sorter.flush();
        }
    }

    private void initAllPageSorters(Statistics<? extends Serializable> chunkStatistic) throws IOException {
        // construct next satisfied page header
        while (chunkDataBuffer.remaining() > 0) {
            // deserialize a PageHeader from chunkDataBuffer
            PageHeader pageHeader;
            if (((byte) (chunkHeader.getChunkType() & 0x3F)) == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER) {
                pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, chunkStatistic);
                // when there is only one page in the chunk, the page statistic is the same as the chunk, so
                // we needn't filter the page again
            } else {
                pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, chunkHeader.getDataType());
            }
            pageSorterList.add(constructPageSorter(pageHeader));
        }
    }

    private void skipCurrentPage(PageHeader pageHeader) {
        chunkDataBuffer.position(chunkDataBuffer.position() + pageHeader.getCompressedSize());
    }

    private PageSorter constructPageSorter(PageHeader pageHeader) throws IOException {
        IUnCompressor unCompressor = IUnCompressor.getUnCompressor(chunkHeader.getCompressionType());
        // record the current position of chunkDataBuffer, use this to get the page data in PageReader
        // through directly accessing the buffer array
        int currentPagePosition = chunkDataBuffer.position();
        skipCurrentPage(pageHeader);
        LazyLoadPageData dataprepare = new LazyLoadPageData(
                chunkDataBuffer.array(), currentPagePosition, unCompressor);
        PageSorter sorter =
                new PageSorter(fileName, (int) (currentPagePosition+startOffset), pageHeader, dataprepare.uncompressPageData(pageHeader));
        return sorter;
    }
}
