package org.apache.datasketches.hll;

import org.apache.datasketches.memory.internal.XxHash64;
import org.testng.annotations.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.datasketches.Util.DEFAULT_UPDATE_SEED;
import static org.apache.datasketches.Util.invPow2;
import static org.apache.datasketches.hash.MurmurHash3.hash;
import static org.apache.datasketches.hll.HllUtil.*;

public class Hll8ArrayCouponHashTest {

    /**
     * 12
     n:1000,estimate:1009,percentErr:0.9
     n:10000,estimate:10165,percentErr:1.65
     n:100000,estimate:99662,percentErr:0.338
     n:10000000,estimate:10075106,percentErr:0.75106
     n:100000000,estimate:99725738,percentErr:0.274262
     14
     n:1000,estimate:992,percentErr:0.8
     n:10000,estimate:9942,percentErr:0.58
     n:100000,estimate:100330,percentErr:0.33
     n:10000000,estimate:9951801,percentErr:0.48199
     n:100000000,estimate:99566517,percentErr:0.433483
     16
     n:1000,estimate:995,percentErr:0.5
     n:10000,estimate:9968,percentErr:0.32
     n:100000,estimate:99977,percentErr:0.023
     n:10000000,estimate:9968780,percentErr:0.3122
     n:100000000,estimate:99714024,percentErr:0.285976
     */
    @Test
    public void testOtherHashErr() {
        long[] ns = new long[]{1000, 10000, 100000, 10000000, 100000000};

        for (long n : ns) {
            Hll8Array hll = new Hll8Array(16);
            for (int i = 0; i < n; i++) {
                String key = i + "";
                long h = XxHash64.hashString(key, 0, key.length(), 0);
                int coupon = coupon(h);
                if ((coupon >>> KEY_BITS_26 ) == EMPTY) {
                    continue;
                }
                hll.couponUpdate(coupon);
                /*final int newValue = coupon >>> KEY_BITS_26;
                final int configKmask = (1 << hll.lgConfigK) - 1;
                final int slotNo = coupon & configKmask;
                hll.updateSlotWithKxQ(slotNo, newValue);*/
            }
            long estimate = (long) hll.getEstimate();
            double percentErr = Math.abs(estimate - n) * 100D / n;
            System.out.println("n:" + n + ",estimate:" + estimate+ ",percentErr:" + percentErr);
        }
    }

    /**
     * 12
     n:1000,estimate:1012,percentErr:1.2
     n:10000,estimate:10309,percentErr:3.09
     n:100000,estimate:100507,percentErr:0.507
     n:10000000,estimate:9901109,percentErr:0.98891
     n:100000000,estimate:100349550,percentErr:0.34955
     14
     n:1000,estimate:992,percentErr:0.8
     n:10000,estimate:9970,percentErr:0.3
     n:100000,estimate:101022,percentErr:1.022
     n:10000000,estimate:9896703,percentErr:1.03297
     n:100000000,estimate:99534273,percentErr:0.465727
     16
     n:1000,estimate:997,percentErr:0.3
     n:10000,estimate:9988,percentErr:0.12
     n:100000,estimate:100162,percentErr:0.162
     n:10000000,estimate:9951764,percentErr:0.48236
     n:100000000,estimate:99138808,percentErr:0.861192
     */
    @Test
    public void testOtherHashNoKxQErr() {
        long[] ns = new long[]{1000, 10000, 100000, 10000000, 100000000};

        for (long n : ns) {
            Hll8Array hll = new Hll8Array(16);
            for (int i = 0; i < n; i++) {
                String key = i + "";
                long h = XxHash64.hashString(key, 0, key.length(), 0);
                int coupon = coupon(h);
                if ((coupon >>> KEY_BITS_26 ) == EMPTY) {
                    continue;
                }
                //hll.couponUpdate(coupon);
                final int newValue = coupon >>> KEY_BITS_26;
                final int configKmask = (1 << hll.lgConfigK) - 1;
                final int slotNo = coupon & configKmask;
                hll.updateSlotNoKxQ(slotNo, newValue);
            }
            hll.putOutOfOrder(true);
            checkRebuildCurMinNumKxQ(hll);
            long estimate = (long) hll.getEstimate();
            double percentErr = Math.abs(estimate - n) * 100D / n;
            System.out.println("n:" + n + ",estimate:" + estimate+ ",percentErr:" + percentErr);
        }
    }

    @Test
    public void test0() {
        Hll8Array hll8 = new Hll8Array(12);

        for (int i = 0; i < 1000000; i++){
            String key = i + "a";
            final byte[] data = key.getBytes(UTF_8);
            int coupon = coupon(hash(data, DEFAULT_UPDATE_SEED));
            if ((coupon >>> KEY_BITS_26 ) == EMPTY) {
                continue;
            }
            hll8.couponUpdate(coupon);
        }

        System.out.println(hll8.toString());
        System.out.println("size:" + hll8.toCompactByteArray().length);
        System.out.println("###################");
        System.out.println("estimate"+ hll8.getEstimate());
        System.out.println("sompositeEstimate"+ hll8.getCompositeEstimate());
    }

    @Test
    public void test() {
        Hll8Array hll8 = new Hll8Array(12);

        for (int i = 0; i < 1000000; i++){
            String key = i + "a";
            final byte[] data = key.getBytes(UTF_8);
            int coupon = coupon(hash(data, DEFAULT_UPDATE_SEED));
            if ((coupon >>> KEY_BITS_26 ) == EMPTY) {
                continue;
            }
            //hll8.couponUpdate(coupon);
            final int newValue = coupon >>> KEY_BITS_26;
            final int configKmask = (1 << hll8.lgConfigK) - 1;
            final int slotNo = coupon & configKmask;
            hll8.updateSlotNoKxQ(slotNo, newValue);
        }
        hll8.putOutOfOrder(true);
        checkRebuildCurMinNumKxQ(hll8);


        System.out.println(hll8.toString());
        System.out.println("size:" + hll8.toCompactByteArray().length);
        System.out.println("###################");
        System.out.println("estimate"+ hll8.getEstimate());
        System.out.println("sompositeEstimate"+ hll8.getCompositeEstimate());
    }


    @Test
    public void testOtherHash() {
        Hll8Array hll8 = new Hll8Array(12);

        for (int i = 0; i < 10000000; i++){
            String key = i + "a";
            long h = XxHash64.hashString(key, 0, key.length(), 0);
            int coupon = coupon(h);
            if ((coupon >>> KEY_BITS_26 ) == EMPTY) {
                continue;
            }
            //hll8.couponUpdate(coupon);
            final int newValue = coupon >>> KEY_BITS_26;
            final int configKmask = (1 << hll8.lgConfigK) - 1;
            final int slotNo = coupon & configKmask;
            hll8.updateSlotNoKxQ(slotNo, newValue);
        }
        hll8.putOutOfOrder(true);
        checkRebuildCurMinNumKxQ(hll8);


        System.out.println(hll8.toString());
        System.out.println("size:" + hll8.toCompactByteArray().length);
        System.out.println("###################");
        System.out.println("estimate"+ hll8.getEstimate());
        System.out.println("sompositeEstimate"+ hll8.getCompositeEstimate());
    }

    @Test
    public void testHashMerge() {
        Hll8Array hll8 = new Hll8Array(12);

        for (int i = 0; i < 1500000; i++){
            String key = i + "a";
            final byte[] data = key.getBytes(UTF_8);
            int coupon = coupon(hash(data, DEFAULT_UPDATE_SEED));
            if ((coupon >>> KEY_BITS_26 ) == EMPTY) {
                continue;
            }
            //hll8.couponUpdate(coupon);
            final int newValue = coupon >>> KEY_BITS_26;
            final int configKmask = (1 << hll8.lgConfigK) - 1;
            final int slotNo = coupon & configKmask;
            hll8.updateSlotNoKxQ(slotNo, newValue);
            //hll8.updateSlotWithKxQ(slotNo, newValue);
        }

        Hll8Array hll82 = new Hll8Array(12);
        for (int i = 0; i < 1000000; i++){
            String key = i + "";
            final byte[] data = key.getBytes(UTF_8);
            int coupon = coupon(hash(data, DEFAULT_UPDATE_SEED));
            if ((coupon >>> KEY_BITS_26 ) == EMPTY) {
                continue;
            }
            //hll8.couponUpdate(coupon);
            final int newValue = coupon >>> KEY_BITS_26;
            final int configKmask = (1 << hll82.lgConfigK) - 1;
            final int slotNo = coupon & configKmask;
            hll82.updateSlotNoKxQ(slotNo, newValue);
            //hll82.updateSlotWithKxQ(slotNo, newValue);
        }

        for (int i = 0; i < 1000000; i++){
            String key = i + "a";
            final byte[] data = key.getBytes(UTF_8);
            int coupon = coupon(hash(data, DEFAULT_UPDATE_SEED));
            if ((coupon >>> KEY_BITS_26 ) == EMPTY) {
                continue;
            }
            //hll8.couponUpdate(coupon);
            final int newValue = coupon >>> KEY_BITS_26;
            final int configKmask = (1 << hll82.lgConfigK) - 1;
            final int slotNo = coupon & configKmask;
            hll82.updateSlotNoKxQ(slotNo, newValue);
            //hll8.updateSlotWithKxQ(slotNo, newValue);
        }

        final int srcK = 1 << hll82.lgConfigK;
        final byte[] srcArr = hll82.hllByteArr;
        final byte[] tgtArr = hll8.hllByteArr;
        for (int i = 0; i < srcK; i++) {
            final byte srcV = srcArr[i];
            final byte tgtV = tgtArr[i];
            tgtArr[i] = (srcV > tgtV) ? srcV : tgtV;
        }

        checkRebuildCurMinNumKxQ(hll8);
        hll8.putOutOfOrder(true);

        System.out.println(hll8.toString());
        System.out.println("size:" + hll8.toCompactByteArray().length);
        System.out.println("###################");
        System.out.println("estimate"+ hll8.getEstimate());
        System.out.println("sompositeEstimate"+ hll8.getCompositeEstimate());
    }

    @Test
    public void testOtherHashMerge() {
        Hll8Array hll8 = new Hll8Array(12);

        for (int i = 0; i < 1500000; i++){
            String key = i + "a";
            long h = XxHash64.hashString(key, 0, key.length(), 0);
            int coupon = coupon(h);
            if ((coupon >>> KEY_BITS_26 ) == EMPTY) {
                continue;
            }
            //hll8.couponUpdate(coupon);
            final int newValue = coupon >>> KEY_BITS_26;
            final int configKmask = (1 << hll8.lgConfigK) - 1;
            final int slotNo = coupon & configKmask;
            hll8.updateSlotNoKxQ(slotNo, newValue);
            //hll8.updateSlotWithKxQ(slotNo, newValue);
        }

        Hll8Array hll82 = new Hll8Array(12);
        for (int i = 0; i < 1000000; i++){
            String key = i + "";
            long h = XxHash64.hashString(key, 0, key.length(), 0);
            int coupon = coupon(h);
            if ((coupon >>> KEY_BITS_26 ) == EMPTY) {
                continue;
            }
            //hll8.couponUpdate(coupon);
            final int newValue = coupon >>> KEY_BITS_26;
            final int configKmask = (1 << hll82.lgConfigK) - 1;
            final int slotNo = coupon & configKmask;
            hll82.updateSlotNoKxQ(slotNo, newValue);
            //hll82.updateSlotWithKxQ(slotNo, newValue);
        }

        for (int i = 0; i < 1000000; i++){
            String key = i + "a";
            long h = XxHash64.hashString(key, 0, key.length(), 0);
            int coupon = coupon(h);
            if ((coupon >>> KEY_BITS_26 ) == EMPTY) {
                continue;
            }
            //hll8.couponUpdate(coupon);
            final int newValue = coupon >>> KEY_BITS_26;
            final int configKmask = (1 << hll82.lgConfigK) - 1;
            final int slotNo = coupon & configKmask;
            hll82.updateSlotNoKxQ(slotNo, newValue);
            //hll8.updateSlotWithKxQ(slotNo, newValue);
        }

        final int srcK = 1 << hll82.lgConfigK;
        final byte[] srcArr = hll82.hllByteArr;
        final byte[] tgtArr = hll8.hllByteArr;
        for (int i = 0; i < srcK; i++) {
            final byte srcV = srcArr[i];
            final byte tgtV = tgtArr[i];
            tgtArr[i] = (srcV > tgtV) ? srcV : tgtV;
        }

        checkRebuildCurMinNumKxQ(hll8);
        hll8.putOutOfOrder(true);

        System.out.println(hll8.toString());
        System.out.println("size:" + hll8.toCompactByteArray().length);
        System.out.println("###################");
        System.out.println("estimate"+ hll8.getEstimate());
        System.out.println("sompositeEstimate"+ hll8.getCompositeEstimate());
    }

    /**
     * 不行，hash不同会认为是不同的值
     */
    @Test
    public void testTwoHashMerge() {
        Hll8Array hll8 = new Hll8Array(12);

        for (int i = 0; i < 1500000; i++){
            String key = i + "a";
            final byte[] data = key.getBytes(UTF_8);
            int coupon = coupon(hash(data, DEFAULT_UPDATE_SEED));
            if ((coupon >>> KEY_BITS_26 ) == EMPTY) {
                continue;
            }
            //hll8.couponUpdate(coupon);
            final int newValue = coupon >>> KEY_BITS_26;
            final int configKmask = (1 << hll8.lgConfigK) - 1;
            final int slotNo = coupon & configKmask;
            hll8.updateSlotNoKxQ(slotNo, newValue);
            //hll8.updateSlotWithKxQ(slotNo, newValue);
        }

        Hll8Array hll82 = new Hll8Array(12);
        for (int i = 0; i < 1000000; i++){
            String key = i + "";
            long h = XxHash64.hashString(key, 0, key.length(), 0);
            int coupon = coupon(h);
            if ((coupon >>> KEY_BITS_26 ) == EMPTY) {
                continue;
            }
            //hll8.couponUpdate(coupon);
            final int newValue = coupon >>> KEY_BITS_26;
            final int configKmask = (1 << hll82.lgConfigK) - 1;
            final int slotNo = coupon & configKmask;
            hll82.updateSlotNoKxQ(slotNo, newValue);
            //hll82.updateSlotWithKxQ(slotNo, newValue);
        }

        for (int i = 0; i < 1000000; i++){
            String key = i + "a";
            long h = XxHash64.hashString(key, 0, key.length(), 0);
            int coupon = coupon(h);
            if ((coupon >>> KEY_BITS_26 ) == EMPTY) {
                continue;
            }
            //hll8.couponUpdate(coupon);
            final int newValue = coupon >>> KEY_BITS_26;
            final int configKmask = (1 << hll82.lgConfigK) - 1;
            final int slotNo = coupon & configKmask;
            hll82.updateSlotNoKxQ(slotNo, newValue);
            //hll8.updateSlotWithKxQ(slotNo, newValue);
        }

        final int srcK = 1 << hll82.lgConfigK;
        final byte[] srcArr = hll82.hllByteArr;
        final byte[] tgtArr = hll8.hllByteArr;
        for (int i = 0; i < srcK; i++) {
            final byte srcV = srcArr[i];
            final byte tgtV = tgtArr[i];
            tgtArr[i] = (srcV > tgtV) ? srcV : tgtV;
        }

        checkRebuildCurMinNumKxQ(hll8);
        hll8.putOutOfOrder(true);

        System.out.println(hll8.toString());
        System.out.println("size:" + hll8.toCompactByteArray().length);
        System.out.println("###################");
        System.out.println("estimate"+ hll8.getEstimate());
        System.out.println("sompositeEstimate"+ hll8.getCompositeEstimate());
    }


    @Test
    public void testOtherHashMergeUseBuildIn() {
        Hll8Array hll8 = new Hll8Array(14);

        for (int i = 0; i < 1500000; i++){
            String key = i + "a";
            long h = XxHash64.hashString(key, 0, key.length(), 0);
            int coupon = coupon(h);
            if ((coupon >>> KEY_BITS_26 ) == EMPTY) {
                continue;
            }
            //hll8.couponUpdate(coupon);
            final int newValue = coupon >>> KEY_BITS_26;
            final int configKmask = (1 << hll8.lgConfigK) - 1;
            final int slotNo = coupon & configKmask;
            hll8.updateSlotNoKxQ(slotNo, newValue);
            //hll8.updateSlotWithKxQ(slotNo, newValue);
        }

        Hll8Array hll82 = new Hll8Array(12);
        for (int i = 0; i < 1000000; i++){
            String key = i + "";
            long h = XxHash64.hashString(key, 0, key.length(), 0);
            int coupon = coupon(h);
            if ((coupon >>> KEY_BITS_26 ) == EMPTY) {
                continue;
            }
            //hll8.couponUpdate(coupon);
            final int newValue = coupon >>> KEY_BITS_26;
            final int configKmask = (1 << hll82.lgConfigK) - 1;
            final int slotNo = coupon & configKmask;
            hll82.updateSlotNoKxQ(slotNo, newValue);
            //hll82.updateSlotWithKxQ(slotNo, newValue);
        }

        for (int i = 0; i < 1000000; i++){
            String key = i + "a";
            long h = XxHash64.hashString(key, 0, key.length(), 0);
            int coupon = coupon(h);
            if ((coupon >>> KEY_BITS_26 ) == EMPTY) {
                continue;
            }
            //hll8.couponUpdate(coupon);
            final int newValue = coupon >>> KEY_BITS_26;
            final int configKmask = (1 << hll82.lgConfigK) - 1;
            final int slotNo = coupon & configKmask;
            hll82.updateSlotNoKxQ(slotNo, newValue);
            //hll8.updateSlotWithKxQ(slotNo, newValue);
        }

        HllSketch sketch1 = new HllSketch(hll8.lgConfigK, TgtHllType.HLL_8);
        sketch1.hllSketchImpl = hll8;
        HllSketch sketch2 = new HllSketch(hll82.lgConfigK, TgtHllType.HLL_8);
        sketch2.hllSketchImpl = hll82;

        // 这里会下采样，按照最小的算
        Union union = new Union(14);
        union.update(sketch1);
        union.update(sketch2);
        HllSketch sketch = union.getResult();

        System.out.println(sketch.toString());
        System.out.println("size:" + sketch.toCompactByteArray().length);
        System.out.println("###################");
        System.out.println("estimate"+ sketch.getEstimate());

        System.out.println("**********************");


    }

    private static final int coupon(final long[] hash) {
        final int addr26 = (int) ((hash[0] & KEY_MASK_26));
        final int lz = Long.numberOfLeadingZeros(hash[1]);
        final int value = ((lz > 62 ? 62 : lz) + 1);
        return (value << KEY_BITS_26) | addr26;
    }

    private static final int coupon(final long hash) {
        final int addr26 = (int) ((hash & KEY_MASK_26));
        final int lz = Long.numberOfLeadingZeros(hash);
        final int value = ((lz > 62 ? 62 : lz) + 1);
        return (value << KEY_BITS_26) | addr26;
    }

    //Used to rebuild curMin, numAtCurMin and KxQ registers, due to high performance merge operation
    static final void checkRebuildCurMinNumKxQ(Hll8Array hllSketchImpl) {
        final boolean rebuild = hllSketchImpl.isRebuildCurMinNumKxQFlag();
        final AbstractHllArray absHllArr = (AbstractHllArray)(hllSketchImpl);
        int curMin = 64;
        int numAtCurMin = 0;
        double kxq0 = 1 << absHllArr.getLgConfigK();
        double kxq1 = 0;
        final PairIterator itr = absHllArr.iterator();
        while (itr.nextAll()) {
            final int v = itr.getValue();
            if (v > 0) {
                if (v < 32) { kxq0 += invPow2(v) - 1.0; }
                else        { kxq1 += invPow2(v) - 1.0; }
            }
            if (v > curMin) { continue; }
            if (v < curMin) {
                curMin = v;
                numAtCurMin = 1;
            } else {
                numAtCurMin++;
            }
        }
        absHllArr.putKxQ0(kxq0);
        absHllArr.putKxQ1(kxq1);
        absHllArr.putCurMin(curMin);
        absHllArr.putNumAtCurMin(numAtCurMin);
        absHllArr.putRebuildCurMinNumKxQFlag(false);
        //HipAccum is not affected
    }
}
