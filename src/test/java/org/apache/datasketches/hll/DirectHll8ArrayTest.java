package org.apache.datasketches.hll;

import org.apache.datasketches.memory.WritableMemory;
import org.testng.annotations.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.datasketches.Util.DEFAULT_UPDATE_SEED;
import static org.apache.datasketches.hash.MurmurHash3.hash;
import static org.apache.datasketches.hll.HllUtil.*;

public class DirectHll8ArrayTest {

    @Test
    public void test() {
        int lgConfigK = 12;
        int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgConfigK, TgtHllType.HLL_8);
        System.out.println(bytes);
        WritableMemory wmem = WritableMemory.allocate(bytes);
        DirectHll8Array hll = new DirectHll8Array(lgConfigK, wmem);
        for (int i = 0; i < 200000; i++){
            String key = i + "";
            final byte[] data = key.getBytes(UTF_8);
            int coupon = coupon(hash(data, DEFAULT_UPDATE_SEED));
            if ((coupon >>> KEY_BITS_26 ) == EMPTY) {
                continue;
            }
            hll.couponUpdate(coupon);
        }

        System.out.println("estimate"+ hll.getEstimate());

    }

    // 直接操作字节数组，可以用于对外内存
    @Test
    public void test2() {
        int lgConfigK = 12;
        int n = 10000000;
        TgtHllType type = TgtHllType.HLL_8;
        int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgConfigK, type);
        System.out.println(bytes);
        WritableMemory wmem = WritableMemory.allocate(bytes);
        HllSketch sk = new HllSketch(lgConfigK, type, wmem);
        for (int i = 0; i < n; i++) {
            String key = i + "";
            sk.update(key);
        }

        System.out.println("estimate"+ sk.getEstimate());

    }


    private static final int coupon(final long[] hash) {
        final int addr26 = (int) ((hash[0] & KEY_MASK_26));
        final int lz = Long.numberOfLeadingZeros(hash[1]);
        final int value = ((lz > 62 ? 62 : lz) + 1);
        return (value << KEY_BITS_26) | addr26;
    }
}
