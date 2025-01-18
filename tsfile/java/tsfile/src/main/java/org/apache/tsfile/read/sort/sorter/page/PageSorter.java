package org.apache.tsfile.read.sort.sorter.page;

import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.read.sort.utils.CompressedBubbleSorter;
import org.apache.tsfile.read.sort.utils.CompressedData;
import org.apache.tsfile.utils.ReadWriteForEncodingUtils;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

public class PageSorter {
    private final PageHeader pageHeader;
    private int offset;
    private String fileName;
    /** time column in memory */
    private CompressedData timeData;
    /** value column in memory */
    private CompressedData valueData;
    private int PAGE_SIZE;

    public PageSorter(String filename,
            int pos,
            PageHeader pageHeader,
            ByteBuffer pageData) {
        this.fileName = filename;
        this.offset = pos;
        this.pageHeader = pageHeader;
        splitDataToTimeStampAndValue(pageData);
    }

    private void splitDataToTimeStampAndValue(ByteBuffer pageData) {
        int timeDataLength = ReadWriteForEncodingUtils.readUnsignedVarInt(pageData);
        offset = offset+pageData.position();
        PAGE_SIZE = (int) pageHeader.getStatistics().getCount();
        ByteBuffer timeBuffer = pageData.slice();
        timeBuffer.limit(timeDataLength);
        timeData = new CompressedData(timeBuffer, PAGE_SIZE);
        ByteBuffer valueBuffer = pageData.slice();
        valueBuffer.position(timeDataLength);
        valueData = new CompressedData(valueBuffer, PAGE_SIZE);
    }

    public void sort() throws IOException {
        CompressedBubbleSorter sorter = new CompressedBubbleSorter(timeData, valueData);
        sorter.blockSort(0, PAGE_SIZE, 8, timeData.getLen()-1, 8, valueData.getLen());
    }

    public void flush() throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(fileName, "rw")) {
            file.seek(offset);

            // 将新数据写入文件，替换原有内容
            file.write(timeData.vals);
            file.write(timeData.lens);

            file.seek(file.getFilePointer() + 4);
            file.write(valueData.vals);
            file.write(valueData.lens);

            System.out.println("sort successfully");
        }
    }
}
