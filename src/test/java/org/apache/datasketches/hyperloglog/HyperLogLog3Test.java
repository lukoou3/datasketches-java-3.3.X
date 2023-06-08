package org.apache.datasketches.hyperloglog;

import org.testng.annotations.Test;

public class HyperLogLog3Test {

    /**
     * 12
     n:1000,estimate:1011,percentErr:1.1
     n:10000,estimate:10346,percentErr:3.46
     n:100000,estimate:99071,percentErr:0.929
     n:10000000,estimate:10078450,percentErr:0.7845
     n:100000000,estimate:103798537,percentErr:3.798537
     true
     n:1000,estimate:1011,percentErr:1.1
     n:10000,estimate:10093,percentErr:0.93
     n:100000,estimate:99071,percentErr:0.929
     n:10000000,estimate:10078450,percentErr:0.7845
     n:100000000,estimate:103798537,percentErr:3.798537

     14
     n:1000,estimate:1000,percentErr:0.0
     n:10000,estimate:10015,percentErr:0.15
     n:100000,estimate:100011,percentErr:0.011
     n:10000000,estimate:9990785,percentErr:0.09215
     n:100000000,estimate:100048685,percentErr:0.048685

     n:1000,estimate:1000,percentErr:0.0
     n:10000,estimate:10015,percentErr:0.15
     n:100000,estimate:100011,percentErr:0.011
     n:10000000,estimate:9990785,percentErr:0.09215
     n:100000000,estimate:100048685,percentErr:0.048685

     16
     n:1000,estimate:1002,percentErr:0.2
     n:10000,estimate:10036,percentErr:0.36
     n:100000,estimate:99718,percentErr:0.282
     n:10000000,estimate:10020750,percentErr:0.2075
     n:100000000,estimate:100197578,percentErr:0.197578

     n:1000,estimate:1002,percentErr:0.2
     n:10000,estimate:10036,percentErr:0.36
     n:100000,estimate:99684,percentErr:0.316
     n:10000000,estimate:10020750,percentErr:0.2075
     n:100000000,estimate:100197578,percentErr:0.197578
     *
     */
    @Test
    public void test() {
        long[] ns = new long[]{1000, 10000, 100000, 1000000, 10000000, 100000000};
        for (long n : ns) {
            HyperLogLog3 hll = new HyperLogLog3(12, true);
            for (int i = 0; i < n; i++) {
                String key = i + "a";
                hll.addString(key);
            }
            long estimate = hll.getEstimate();
            double percentErr = Math.abs(estimate - n) * 100D / n;
            System.out.println("n:" + n + ",estimate:" + estimate+ ",percentErr:" + percentErr);
        }
    }

    @Test
    public void testMerge() {
        HyperLogLog3 hll1 = new HyperLogLog3(12, true);
        for (int i = 0; i < 10000000; i++) {
            String key = i + "";
            hll1.addString(key);
            hll1.addString(key);
        }

        HyperLogLog3 hll2 = new HyperLogLog3(12, true);
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
        HyperLogLog3 hll1 = new HyperLogLog3(12, true);
        for (int i = 0; i < 10000000; i++) {
            String key = i + "";
            hll1.addString(key);
            hll1.addString(key);
        }

        HyperLogLog3 hll2 = new HyperLogLog3(14, true);
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
        HyperLogLog3 hll1 = new HyperLogLog3(14, true);
        for (int i = 0; i < 10000000; i++) {
            String key = i + "";
            hll1.addString(key);
            hll1.addString(key);
        }

        HyperLogLog3 hll2 = new HyperLogLog3(12, true);
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
