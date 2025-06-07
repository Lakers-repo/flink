package org.apache.flink.table.examples.java.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Codec {

    // not support encode negative value now
    public static void encodeVarint64(long source, DataOutput out) throws IOException {
        assert source >= 0;
        short b = 128;

        while (source >= b) {
            out.write((int) (source & (b - 1) | b));
            source = source >> 7;
        }
        out.write((int) (source & (b - 1)));
    }

    // not support decode negative value now
    public static long decodeVarint64(DataInput in) throws IOException {
        long result = 0;
        int shift = 0;
        short b = 128;

        while (true) {
            int oneByte = in.readUnsignedByte();
            boolean isEnd = (oneByte & b) == 0;
            result = result | ((long) (oneByte & b - 1) << (shift * 7));
            if (isEnd) {
                break;
            }
            shift++;
        }

        return result;
    }
}
