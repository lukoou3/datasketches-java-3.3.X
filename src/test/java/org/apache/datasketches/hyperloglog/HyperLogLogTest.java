package org.apache.datasketches.hyperloglog;


import org.testng.annotations.Test;

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

}
