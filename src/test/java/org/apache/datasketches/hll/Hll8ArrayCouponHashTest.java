package org.apache.datasketches.hll;

import org.apache.datasketches.memory.internal.XxHash64;
import org.testng.annotations.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.datasketches.Util.DEFAULT_UPDATE_SEED;
import static org.apache.datasketches.Util.invPow2;
import static org.apache.datasketches.hash.MurmurHash3.hash;
import static org.apache.datasketches.hll.HllUtil.*;

public class Hll8ArrayCouponHashTest {

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
