package org.apache.tsfile.read.sort.sorter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class LocalTsFileInOutput implements TsFileInOutput {
    private final FileChannel channel;  // 用于文件的随机读写操作
    private final String filePath;      // 文件路径

    // 构造函数
    public LocalTsFileInOutput(Path file) throws IOException {
        this.channel = FileChannel.open(
                file,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE
        );
        this.filePath = file.toString();
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        return channel.read(dst); // 从文件的当前位置读取数据
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        return channel.read(dst, position); // 从指定位置读取数据
    }

    @Override
    public void write(byte[] b) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(b); // 将字节数组包装为 ByteBuffer
        while (buffer.hasRemaining()) {
            channel.write(buffer); // 写入数据到文件当前位置
        }
    }

    @Override
    public void write(byte[] b, long position) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(b); // 将字节数组包装为 ByteBuffer
        while (buffer.hasRemaining()) {
            channel.write(buffer, position); // 从指定位置写入数据
            position += buffer.remaining();
        }
    }

    // 关闭 FileChannel 释放资源
    public void close() throws IOException {
        channel.close();
    }

    // 获取文件路径
    public String getFilePath() {
        return filePath;
    }
}