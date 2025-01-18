package org.apache.tsfile.read.sort.sorter;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface TsFileInOutput {

    int read(ByteBuffer dst) throws IOException;

    int read(ByteBuffer dst, long position) throws IOException;

    void write(byte[] b) throws IOException;

    void write(byte[] b, long position) throws IOException;
}
