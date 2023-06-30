package org.apache.datasketches.hlld;

import java.nio.ByteBuffer;

public class Hll {
    static final int DEFAULT_PRECISION = 12;
    static final int HLL_MIN_PRECISION = 4; // 16 registers
    static final int HLL_MAX_PRECISION = 18; // 262,144 registers

    static final long DEFAULT_HASH_SEED = 0L; // 9001L

    HllImpl hllImpl = null;

    public Hll() {
        this(DEFAULT_PRECISION);
    }

    public Hll(int precision) {
        if (precision > HLL_MAX_PRECISION || precision < HLL_MIN_PRECISION) {
            throw new RuntimeException("precision");
        }
        hllImpl = new HllIntArray(precision);
    }

    public Hll(int precision, ByteBuffer byteBuffer) {
        hllImpl = new DirectHllIntArray(precision, byteBuffer);
    }

    public void add(long val) {
        hllImpl.add(val);
    }

    public void add(double val) {
        hllImpl.add(val);
    }

    public void add(String val) {
        hllImpl.add(val);
    }

    public void add(byte[] val) {
        hllImpl.add(val);
    }

    public void reset() {
        hllImpl.reset();
    }

    public double getEstimate() {
        return hllImpl.getEstimate();
    }

    public double size() {
        return hllImpl.size();
    }


}
