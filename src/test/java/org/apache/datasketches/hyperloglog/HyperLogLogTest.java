package org.apache.datasketches.hyperloglog;


import org.testng.annotations.Test;

import java.util.Arrays;

public class HyperLogLogTest {

    /**
     *
     *
     12
     n:1000,estimate:1012,percentErr:1.2
     n:10000,estimate:10425,percentErr:4.25
     n:100000,estimate:102387,percentErr:2.387
     n:10000000,estimate:10063980,percentErr:0.6398
     n:100000000,estimate:97723304,percentErr:2.276696
     true
     n:1000,estimate:1012,percentErr:1.2
     n:10000,estimate:10181,percentErr:1.81
     n:100000,estimate:102387,percentErr:2.387
     n:10000000,estimate:10063980,percentErr:0.6398
     n:100000000,estimate:97723304,percentErr:2.276696


     14
     n:1000,estimate:992,percentErr:0.8
     n:10000,estimate:9971,percentErr:0.29
     n:100000,estimate:100099,percentErr:0.099
     n:10000000,estimate:9960392,percentErr:0.39608
     n:100000000,estimate:98343379,percentErr:1.656621

     n:1000,estimate:992,percentErr:0.8
     n:10000,estimate:9971,percentErr:0.29
     n:100000,estimate:100099,percentErr:0.099
     n:10000000,estimate:9960392,percentErr:0.39608
     n:100000000,estimate:98343379,percentErr:1.656621

     16
     n:1000,estimate:998,percentErr:0.2
     n:10000,estimate:9989,percentErr:0.11
     n:100000,estimate:100154,percentErr:0.154
     n:10000000,estimate:9977524,percentErr:0.22476
     n:100000000,estimate:99486087,percentErr:0.513913

     n:1000,estimate:998,percentErr:0.2
     n:10000,estimate:9989,percentErr:0.11
     n:100000,estimate:100353,percentErr:0.353
     n:10000000,estimate:9977524,percentErr:0.22476
     n:100000000,estimate:99486087,percentErr:0.513913
     */
    @Test
    public void test() {
        long[] ns = new long[]{1000, 10000, 100000, 10000000, 100000000};
        for (long n : ns) {
            HyperLogLog hll = new HyperLogLog(16, true);
            for (int i = 0; i < n; i++) {
                String key = i + "";
                hll.addString(key);
                hll.addString(key);
            }
            long estimate = hll.getEstimate();
            double percentErr = Math.abs(estimate - n) * 100D / n;
            System.out.println("n:" + n + ",estimate:" + estimate+ ",percentErr:" + percentErr);
        }
    }

    @Test
    public void testSer() {
        HyperLogLog hll = new HyperLogLog(12, true);
        for (int i = 0; i < 10000000; i++) {
            String key = i + "";
            hll.addString(key);
            hll.addString(key);
        }

        byte[] bytes = hll.toBytes();
        HyperLogLog hll2 = HyperLogLog.fromBytes(bytes);
        System.out.println(hll.getEstimate());
        System.out.println(hll2.getEstimate());
        System.out.println(Arrays.equals(hll.regs, hll2.regs) );
    }

    @Test
    public void testSerBase64() {
        HyperLogLog hll = new HyperLogLog(12, true);
        for (int i = 0; i < 10000000; i++) {
            String key = i + "";
            hll.addString(key);
            hll.addString(key);
        }

        String base64Str = hll.toBase64String();
        System.out.println(base64Str);
        System.out.println(base64Str.length());
        HyperLogLog hll2 = HyperLogLog.fromBase64String(base64Str);
        System.out.println(hll.getEstimate());
        System.out.println(hll2.getEstimate());
        System.out.println(Arrays.equals(hll.regs, hll2.regs) );
    }

    @Test
    public void testMerge() {
        HyperLogLog hll1 = new HyperLogLog(12, true);
        for (int i = 0; i < 10000000; i++) {
            String key = i + "";
            hll1.addString(key);
            hll1.addString(key);
        }

        HyperLogLog hll2 = new HyperLogLog(12, true);
        for (int i = 0; i < 10000000; i++) {
            String key = i + "a";
            hll2.addString(key);
            hll2.addString(key);
        }

        System.out.println(hll1.getEstimate());
        System.out.println(hll2.getEstimate());
        hll1.merge(hll2);
        System.out.println(hll1.getEstimate());

    }

    @Test
    public void testMerge2() {

        HyperLogLog hll1 = new HyperLogLog(12, true);
        for (int i = 0; i < 10000000; i++) {
            String key = i + "";
            hll1.addString(key);
            hll1.addString(key);
        }

        HyperLogLog hll2 = new HyperLogLog(12, true);
        for (int i = 0; i < 10000000; i++) {
            String key = i + "a";
            hll2.addString(key);
            hll2.addString(key);
        }

        System.out.println(hll1.getEstimate());
        System.out.println(hll2.getEstimate());
        hll1.merge(hll2);
        System.out.println(hll1.getEstimate());

    }


}
