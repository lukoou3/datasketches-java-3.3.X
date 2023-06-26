package org.apache.datasketches.hlld;

public class HllUnion {
    final int maxP;
    private Hll impl;

    public HllUnion() {
        this(Hll.DEFAULT_PRECISION);
    }
    public HllUnion(int maxP) {
        this.maxP = maxP;
        impl = new Hll(this.maxP);
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
}
