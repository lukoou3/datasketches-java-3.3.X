package org.apache.datasketches.hll;

import org.apache.datasketches.Family;
import org.testng.annotations.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.datasketches.Util.DEFAULT_UPDATE_SEED;
import static org.apache.datasketches.Util.invPow2;
import static org.apache.datasketches.hash.MurmurHash3.hash;
import static org.apache.datasketches.hll.HllUtil.*;
import static org.apache.datasketches.hll.TgtHllType.HLL_8;

public class Hll8ArrayTest {

    @Test
    public void test() {
        Hll8Array hll8 = new Hll8Array(12);
        for (int i = 0; i < 1000000; i++){
            String key = i + "";
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

    /**
     * 可以直接更新slotNo, newValue，是不是意味着可以其它语言实现的hll，转换成Hll8Array
     */
    @Test
    public void test2() {
        Hll8Array hll8 = new Hll8Array(12);

        for (int i = 0; i < 200000; i++){
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
        }
        System.out.println("estimate"+ hll82.getEstimate());


        final int srcK = 1 << hll82.lgConfigK;
        final byte[] srcArr = hll82.hllByteArr;
        final byte[] tgtArr = hll8.hllByteArr;
        for (int i = 0; i < srcK; i++) {
            final byte srcV = srcArr[i];
            final byte tgtV = tgtArr[i];
            tgtArr[i] = (srcV > tgtV) ? srcV : tgtV;
        }

        checkRebuildCurMinNumKxQ(hll8);

        //hll8.putRebuildCurMinNumKxQFlag(true);
        //hll8.putOutOfOrder(true);
        System.out.println(hll8.toString());
        System.out.println("size:" + hll8.toCompactByteArray().length);
        System.out.println("###################");
        System.out.println("estimate"+ hll8.getEstimate());
        System.out.println("sompositeEstimate"+ hll8.getCompositeEstimate());
    }

    @Test
    public void test21() {
        Hll8Array hll8 = new Hll8Array(12);

        for (int i = 0; i < 200000; i++){
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
        System.out.println("estimate"+ hll82.getEstimate());



        HllSketch sketch1 = new HllSketch(12, TgtHllType.HLL_8);
        sketch1.hllSketchImpl = hll8;
        HllSketch sketch2 = new HllSketch(12, TgtHllType.HLL_8);
        sketch2.hllSketchImpl = hll82;

       /* HllSketchImpl copy = sketch2.hllSketchImpl.copyAs(TgtHllType.HLL_8);
        sketch2.hllSketchImpl = copy;

        HllSketchImpl sketch3 = Union.unionImpl(sketch1, sketch2, 12);
        //Union.mergeHlltoHLLmode(sketch1, sketch2, 12, 12, false, false);
        System.out.println("sketch2 estimate"+ sketch3.getEstimate());*/


        Union union = new Union(12);
        union.update(sketch1);
        union.update(sketch2);
        HllSketch sketch = union.getResult(TgtHllType.HLL_8);

        System.out.println(sketch.toString());
        System.out.println("size:" + sketch.toCompactByteArray().length);
        System.out.println("###################");
        System.out.println("estimate"+ sketch.getEstimate());

        System.out.println("**********************");

    }

    @Test
    public void test22() {
        Hll8Array hll8 = new Hll8Array(12);

        for (int i = 0; i < 200000; i++){
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
        System.out.println("estimate"+ hll82.getEstimate());



        HllSketch sketch1 = new HllSketch(12, TgtHllType.HLL_8);
        sketch1.hllSketchImpl = hll8;
        HllSketch sketch2 = new HllSketch(12, TgtHllType.HLL_8);
        sketch2.hllSketchImpl = hll82;

        HllSketchImpl copy = sketch1.hllSketchImpl.copyAs(TgtHllType.HLL_8);
        sketch1.hllSketchImpl = copy;

        HllSketchImpl impl = Union.unionImpl(sketch2, sketch1, 12);
        HllSketch sketch3 = new HllSketch(12, TgtHllType.HLL_8);
        sketch3.hllSketchImpl = impl;
        Union.checkRebuildCurMinNumKxQ(sketch3);
        //Union.mergeHlltoHLLmode(sketch1, sketch2, 12, 12, false, false);
        System.out.println("sketch2 estimate"+ sketch3.getEstimate());




        Union union = new Union(12);
        union.update(sketch1);
        union.update(sketch2);
        HllSketch sketch = union.getResult(TgtHllType.HLL_8);

        System.out.println(sketch.toString());
        System.out.println("size:" + sketch.toCompactByteArray().length);
        System.out.println("###################");
        System.out.println("estimate"+ sketch.getEstimate());

        System.out.println("**********************");

    }

    @Test
    public void test4() {
        HllSketch sketch1 = new HllSketch(12, TgtHllType.HLL_8);
        sketch1.hllSketchImpl = new Hll8Array(12);
        for (int i = 0; i < 200000; i++){
            String key = i + "a";
            final byte[] data = key.getBytes(UTF_8);
            int coupon = coupon(hash(data, DEFAULT_UPDATE_SEED));
            if ((coupon >>> KEY_BITS_26 ) == EMPTY) {
                continue;
            }
            sketch1.hllSketchImpl.couponUpdate(coupon);
        }

        HllSketch sketch2 = new HllSketch(12, TgtHllType.HLL_8);
        sketch2.hllSketchImpl = new Hll8Array(12);
        for (int i = 0; i < 1000000; i++){
            String key = i + "";
            final byte[] data = key.getBytes(UTF_8);
            int coupon = coupon(hash(data, DEFAULT_UPDATE_SEED));
            if ((coupon >>> KEY_BITS_26 ) == EMPTY) {
                continue;
            }
            //sketch2.hllSketchImpl.couponUpdate(coupon);
            final int newValue = coupon >>> KEY_BITS_26;
            final int configKmask = (1 << sketch2.hllSketchImpl.lgConfigK) - 1;
            final int slotNo = coupon & configKmask;
            ((Hll8Array)sketch2.hllSketchImpl).updateSlotWithKxQ(slotNo, newValue);
        }

        System.out.println(sketch2);
        System.out.println(sketch2.getEstimate());
        System.out.println(sketch2.hllSketchImpl.getEstimate());

        Union union = new Union(12);
        union.update(sketch2);
        union.update(sketch1);

        HllSketch sketch = union.getResult(TgtHllType.HLL_8);

        System.out.println(sketch);
        System.out.println("###################");
        sketch.getEstimate();
        System.out.println(sketch.getEstimate());

        System.out.println("###################");

    }

    @Test
    public void test3() {
        HllSketch sketch1 = new HllSketch(12, TgtHllType.HLL_8);
        for (int i = 0; i < 200000; i++){
            String key = i + "a";
            sketch1.update(key);
        }

        HllSketch sketch2 = new HllSketch(12, TgtHllType.HLL_8);
        for (int i = 0; i < 1000000; i++){
            String key = i + "";
            sketch2.update(key );
        }

        // 合并后误差看不出来
        Union union = new Union(12);
        union.update(sketch1);
        union.update(sketch2);

        HllSketch sketch = union.getResult(TgtHllType.HLL_8);

        System.out.println(sketch);
        System.out.println("###################");
        sketch.getEstimate();
        System.out.println(sketch.getEstimate());

        System.out.println("###################");
    }

    @Test
    public void testHllSketch() {
        HllSketch sketch = new HllSketch(12);
        for (int key = 0; key < 1000000; key++){
            sketch.update(key + "");
        }
        sketch.getEstimate();
        System.out.println(sketch.toString());
        System.out.println("size:" + sketch.toCompactByteArray().length);
        System.out.println("###################");
        System.out.println("estimate"+ sketch.getEstimate());
    }

    private static final int coupon(final long[] hash) {
        final int addr26 = (int) ((hash[0] & KEY_MASK_26));
        final int lz = Long.numberOfLeadingZeros(hash[1]);
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
