package org.apache.datasketches.hlld2;

import java.io.Serializable;
import java.nio.ByteBuffer;

public class HllUnion implements Serializable {
    private int maxP;
    private Hll impl;

    public HllUnion() {
        this(Hll.DEFAULT_PRECISION);
    }
    public HllUnion(int maxP) {
        this.maxP = maxP;
        impl = new Hll(this.maxP);
    }

    private HllUnion(Hll hll) {
        this.maxP = hll.p;
        impl = hll;
    }

    public Hll getResult() {
        return impl;
    }

    public void update(final Hll hll) {
        impl = unionImpl(hll, impl);
    }

    private static Hll unionImpl(Hll source, Hll dest) {
        if (source == null) {
            return dest;
        }

        if (dest.p < source.p) {
            source = source.squash(dest.p);
        }else if (dest.p > source.p){
            dest = dest.squash(source.p);
        }

        int srcV, tgtV;
        for (int i = 0; i < source.reg; i++) {

            srcV = source.getRegister(i);
            tgtV = dest.getRegister(i);
            if (srcV > tgtV) {
                dest.setRegister(i, srcV);
            }
        }

        return dest;
    }

    public static final int getMaxSerializationBytes(final int precision){
        return Hll.getSerializationBytes(precision);
    }

    public static HllUnion fromByteBuffer(ByteBuffer byteBuffer) {
        Hll hll = Hll.fromByteBuffer(byteBuffer);
        return new HllUnion(hll);
    }

    public byte[] toBytes() {
        return impl.toBytes();
    }

    public int getMaxP() {
        return maxP;
    }

    public void setMaxP(int maxP) {
        this.maxP = maxP;
    }

    public Hll getImpl() {
        return impl;
    }

    public void setImpl(Hll impl) {
        this.impl = impl;
    }
}
