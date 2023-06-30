package org.apache.datasketches.hlld;

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

    public HllUnion(int maxP, ByteBuffer byteBuffer) {
        this.maxP = maxP;
        impl = new Hll(this.maxP, byteBuffer);
    }

    public Hll getResult() {
        return impl;
    }

    public void update(final Hll hll) {
        impl.hllImpl = unionImpl(hll, impl);
    }

    private static HllImpl unionImpl(Hll source, Hll dest) {
        if (source == null) {
            return dest.hllImpl;
        }

        HllImpl descHllImpl = dest.hllImpl;
        HllImpl sourceHllImpl = source.hllImpl;
        int destP = descHllImpl.getPrecision();
        int sourceP = sourceHllImpl.getPrecision();
        HllImpl hllImpl = null;

        if (destP < sourceP) {
            sourceHllImpl = squash(sourceHllImpl, destP);
            merge(sourceHllImpl, descHllImpl);
            hllImpl = descHllImpl;
        } else if (destP > sourceP) {
            descHllImpl = squash(descHllImpl, sourceP);
            merge(sourceHllImpl, descHllImpl);
            if (!dest.hllImpl.isMemory()) {
                hllImpl = descHllImpl;
            } else {
                // 使用ByteBuffer, 可能是堆外内存
                hllImpl = dest.hllImpl;
                hllImpl.setPrecision(descHllImpl.getPrecision());
                hllImpl.reset();
                updateRegisters(descHllImpl, hllImpl);
            }
        } else {
            merge(sourceHllImpl, descHllImpl);
            hllImpl = descHllImpl;
        }

        return hllImpl;
    }

    private static void merge(HllImpl sourceHllImpl, HllImpl descHllImpl) {
        int srcV, tgtV, reg = 1 << sourceHllImpl.getPrecision();
        for (int i = 0; i < reg; i++) {
            srcV = sourceHllImpl.getRegister(i);
            tgtV = descHllImpl.getRegister(i);
            if (srcV > tgtV) {
                descHllImpl.setRegister(i, srcV);
            }
        }
    }

    private static void updateRegisters(HllImpl sourceHllImpl, HllImpl descHllImpl) {
        int srcV, tgtV, reg = 1 << sourceHllImpl.getPrecision();
        for (int i = 0; i < reg; i++) {
            srcV = sourceHllImpl.getRegister(i);
            descHllImpl.setRegister(i, srcV);
        }
    }

    // p, 64 - p
    // +-------------|-------------+
    // |1000000000000|000000001xxxx|  (lr=9 + idx=1024)
    // +-------------|-------------+
    //                \
    // +---------------|-----------+
    // |00000000000|01000000001xxxx|  (lr=2 + idx=0)
    // +---------------|-----------+
    private static HllImpl squash(HllImpl hll, final int p0) {
        int p = hll.getPrecision();
        int reg = 1 << p;
        if (p0 > p) {
            throw new IllegalArgumentException(
                    "HyperLogLog cannot be be squashed to be bigger. Current: "
                            + p + " Provided: " + p0);
        }

        if (p0 == p) {
            return hll;
        }

        final HllImpl dest = new HllIntArray(p0);

        for (int idx = 0; idx < reg; idx++) {
            int regVal = hll.getRegister(idx); // this can be a max of 65, never > 127
            if (regVal != 0) {
                dest.addHash(((long) idx << (64 - p)) | (1L << (64 - p - regVal)));
            }
        }

        return dest;
    }
}
