package org.apache.datasketches.hlld;


import org.testng.annotations.Test;

import java.nio.ByteBuffer;

public class HllTest {

    @Test
    public void testSize() {
        long[] ns = new long[]{100, 1000, 10000, 100000, 10000000, 100000000};
        for (long n : ns) {
            Hll hll = new Hll(14);
            for (int i = 0; i < n; i++) {
                String key = i + "";
                hll.add(key);
            }
            long estimate = Math.round(hll.size()) ;
            double percentErr = Math.abs(estimate - n) * 100D / n;
            System.out.println("n:" + n + ",estimate:" + estimate+ ",percentErr:" + percentErr);
        }
    }

    @Test
    public void testSizeByteBuffer() {
        long[] ns = new long[]{100, 1000, 10000, 100000, 10000000, 100000000};
        for (long n : ns) {
            int p = 14;
            int bytes = DirectHllIntArray.getUpdatableSerializationBytes(p);
            Hll hll = new Hll(p, ByteBuffer.allocate(bytes));
            for (int i = 0; i < n; i++) {
                String key = i + "";
                hll.add(key);
            }
            long estimate = Math.round(hll.size()) ;
            double percentErr = Math.abs(estimate - n) * 100D / n;
            System.out.println("n:" + n + ",estimate:" + estimate+ ",percentErr:" + percentErr);
        }
    }
}
