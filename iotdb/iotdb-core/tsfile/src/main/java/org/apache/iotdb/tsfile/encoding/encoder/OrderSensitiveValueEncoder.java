package org.apache.iotdb.tsfile.encoding.encoder;

import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class OrderSensitiveValueEncoder extends Encoder {
    byte[] lens;
    int valNum = 0;
    boolean isFirst = true;

    public OrderSensitiveValueEncoder() {
        super(TSEncoding.ORDER_SENSITIVE_VALUE);
    }

    public void encode(long value, ByteArrayOutputStream out){
        if (isFirst) {
            lens = new byte[4];
            if (value <= 0) value = -2 * value + 1;
            else value = 2 * value;
            writeBits(value, out);
            isFirst = false;
        }
        else {
            int byteNum = mapDataToLen(value);
            if (value <= 0) value = -2 * value + 1;
            else value = 2 * value;
            writeBits(value, byteNum, out);
        }
        valNum++;
    }

    @Override
    public void flush(ByteArrayOutputStream out) throws IOException {
        // flush control bits
        out.write(lens,0, (valNum+3)/4);
        // flush valNum
        int byteNum = 4;
        while(byteNum>0) {
            out.write(valNum);
            valNum = valNum>>8;
            byteNum--;
        }
    }

    public int mapDataToLen(long data) {
        // zigzag mapping data to length
        // Provide options for lengths of 1, 2, 4, and 8
        if (data <= 0){
            data = data-1;
            data = -data;
        }
        if(data<128) return 1;
        if(data<32768) return 2;
        if(data-1<Integer.MAX_VALUE) return 4;
        return 8;
    }

    private void writeBits(long value, int byteNum, ByteArrayOutputStream out){
        // update control bits
        if(valNum/4 >= lens.length-1){
            byte[] expanded = new byte[lens.length*2];
            System.arraycopy(lens, 0, expanded, 0, lens.length);
            this.lens = expanded;
        }
        byte temp = 0;
        if(byteNum == 1) temp= 1;
        if(byteNum == 2) temp = 2;
        if(byteNum == 4) temp = 3;
        lens[valNum/4] = (byte) (lens[valNum/4]|(temp<<(2*(3-valNum%4))));
        // update data bits
        while(byteNum>0) {
            out.write((int)value);
            value = value>>8;
            byteNum--;
        }
    }

    private void writeBits(long value, ByteArrayOutputStream out) {
        int byteNum = 8;
        while(byteNum>0) {
            out.write((int)value);
            value = value>>8;
            byteNum--;
        }
    }
}
