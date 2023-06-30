package org.apache.datasketches.hlld;

import org.testng.annotations.Test;

import java.nio.ByteBuffer;

import static org.apache.datasketches.hlld.Hll.REG_PER_WORD;

public class DirectHllTest {

    @Test
    public void testSize() {
        long[] ns = new long[]{100, 1000, 10000, 100000, 10000000, 100000000};
        for (long n : ns) {
            int p = 14;
            int reg = 1 << p;
            int words = (reg + REG_PER_WORD - 1) / REG_PER_WORD;
            ByteBuffer byteBuffer = ByteBuffer.allocate(2 + words * 4);
            DirectHll hll = new DirectHll(p, byteBuffer);
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
