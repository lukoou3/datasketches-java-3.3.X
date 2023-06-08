package org.apache.datasketches.hyperloglog;

import org.testng.annotations.Test;

public class HyperLogLog2Test {

    /**
     * 12
     n:1000,estimate:1012,percentErr:1.2
     n:10000,estimate:10551,percentErr:5.51
     n:100000,estimate:100502,percentErr:0.502
     n:10000000,estimate:9900555,percentErr:0.99445
     n:100000000,estimate:100343935,percentErr:0.343935
     true
     n:1000,estimate:1012,percentErr:1.2
     n:10000,estimate:10314,percentErr:3.14
     n:100000,estimate:100502,percentErr:0.502
     n:10000000,estimate:9900555,percentErr:0.99445
     n:100000000,estimate:100343935,percentErr:0.343935

     14
     n:1000,estimate:992,percentErr:0.8
     n:10000,estimate:9971,percentErr:0.29
     n:100000,estimate:101019,percentErr:1.019
     n:10000000,estimate:9896125,percentErr:1.03875
     n:100000000,estimate:99528463,percentErr:0.471537

     n:1000,estimate:992,percentErr:0.8
     n:10000,estimate:9971,percentErr:0.29
     n:100000,estimate:101019,percentErr:1.019
     n:10000000,estimate:9896125,percentErr:1.03875
     n:100000000,estimate:99528463,percentErr:0.471537

     16
     n:1000,estimate:998,percentErr:0.2
     n:10000,estimate:9989,percentErr:0.11
     n:100000,estimate:100154,percentErr:0.154
     n:10000000,estimate:9951207,percentErr:0.48793
     n:100000000,estimate:99133255,percentErr:0.866745

     n:1000,estimate:998,percentErr:0.2
     n:10000,estimate:9989,percentErr:0.11
     n:100000,estimate:100349,percentErr:0.349
     n:10000000,estimate:9951207,percentErr:0.48793
     n:100000000,estimate:99133255,percentErr:0.866745
     *
     */
    @Test
    public void test() {
        long[] ns = new long[]{1000, 10000, 100000, 10000000, 100000000};
        for (long n : ns) {
            HyperLogLog2 hll = new HyperLogLog2(16, true);
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
    public void testMerge() {
        HyperLogLog2 hll1 = new HyperLogLog2(12, true);
        for (int i = 0; i < 10000000; i++) {
            String key = i + "";
            hll1.addString(key);
            hll1.addString(key);
        }

        HyperLogLog2 hll2 = new HyperLogLog2(12, true);
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
        HyperLogLog2 hll1 = new HyperLogLog2(12, true);
        for (int i = 0; i < 10000000; i++) {
            String key = i + "";
            hll1.addString(key);
            hll1.addString(key);
        }

        HyperLogLog2 hll2 = new HyperLogLog2(14, true);
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
    public void testMerge3() {
        HyperLogLog2 hll1 = new HyperLogLog2(14, true);
        for (int i = 0; i < 10000000; i++) {
            String key = i + "";
            hll1.addString(key);
            hll1.addString(key);
        }

        HyperLogLog2 hll2 = new HyperLogLog2(12, true);
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
