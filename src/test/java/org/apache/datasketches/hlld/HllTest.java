package org.apache.datasketches.hlld;


import org.apache.datasketches.memory.internal.XxHash64;
import org.testng.annotations.Test;

public class HllTest {

    /**
     *
     12
     n:1000,estimate:1000,percentErr:0.0
     n:10000,estimate:10007,percentErr:0.07
     n:100000,estimate:101795,percentErr:1.795
     n:10000000,estimate:9906519,percentErr:0.93481
     n:100000000,estimate:102053536,percentErr:2.053536

     14
     n:1000,estimate:1006,percentErr:0.6
     n:10000,estimate:9978,percentErr:0.22
     n:100000,estimate:100906,percentErr:0.906
     n:10000000,estimate:9939345,percentErr:0.60655
     n:100000000,estimate:100924578,percentErr:0.924578

     15
     n:1000,estimate:1003,percentErr:0.3
     n:10000,estimate:9992,percentErr:0.08
     n:100000,estimate:100654,percentErr:0.654
     n:10000000,estimate:9961148,percentErr:0.38852
     n:100000000,estimate:100066148,percentErr:0.066148

     16
     n:1000,estimate:999,percentErr:0.1
     n:10000,estimate:10012,percentErr:0.12
     n:100000,estimate:100623,percentErr:0.623
     n:10000000,estimate:9943592,percentErr:0.56408
     n:100000000,estimate:100050177,percentErr:0.050177
     */
    @Test
    public void testSize() {
        long[] ns = new long[]{100, 1000, 10000, 100000, 10000000, 100000000};
        for (long n : ns) {
            Hll hll = new Hll(12);
            for (int i = 0; i < n; i++) {
                String key = i + "";
                hll.addString(key);
                hll.addString(key);
            }
            long estimate = Math.round(hll.size()) ;
            double percentErr = Math.abs(estimate - n) * 100D / n;
            System.out.println("n:" + n + ",estimate:" + estimate+ ",percentErr:" + percentErr);
        }
    }



    @Test
    public void testMerge() {
        Hll hll1 = new Hll(12);
        for (int i = 0; i < 10000000; i++) {
            String key = i + "";
            hll1.addString(key);
            hll1.addString(key);
        }

        Hll hll2 = new Hll(14);
        for (int i = 0; i < 10000000; i++) {
            //String key = i + "" ;
            String key = i + "" + "a";
            hll2.addString(key);
            hll2.addString(key);
        }

        System.out.println((long)hll1.size());
        System.out.println((long)hll2.size());

        HllUnion union = new HllUnion(12);
        union.update(hll1);
        union.update(hll2);

        System.out.println((long) union.getResult().size());

    }

    @Test
    public void testMerge2() {
        Hll hll1 = new Hll(14);
        for (int i = 0; i < 10000000; i++) {
            String key = i + "";
            hll1.addString(key);
            hll1.addString(key);
        }

        Hll hll2 = new Hll(12);
        for (int i = 0; i < 10000000; i++) {
            //String key = i + "" ;
            String key = i + "" + "a";
            hll2.addString(key);
            hll2.addString(key);
        }

        System.out.println((long)hll1.size());
        System.out.println((long)hll2.size());

        HllUnion union = new HllUnion(14);
        union.update(hll1);
        union.update(hll2);

        System.out.println((long) union.getResult().size());

    }

    @Test
    public void testMerge3() {
        Hll hll1 = new Hll(14);
        for (int i = 0; i < 10000000; i++) {
            String key = i + "";
            hll1.addString(key);
            hll1.addString(key);
        }

        Hll hll2 = new Hll(13);
        for (int i = 0; i < 10000000; i++) {
            //String key = i + "" ;
            String key = i + "" + "a";
            hll2.addString(key);
            hll2.addString(key);
        }

        Hll hll3 = new Hll(13);
        for (int i = 0; i < 10000000; i++) {
            //String key = i + "" ;
            String key = i  + "b";
            hll3.addString(key);
            hll3.addString(key);
        }

        System.out.println((long)hll1.size());
        System.out.println((long)hll2.size());
        System.out.println((long)hll3.size());

        HllUnion union = new HllUnion(14);
        union.update(hll1);
        union.update(hll2);
        union.update(hll3);

        System.out.println((long) union.getResult().size());

    }


    @Test
    public void testSquash2() {
        int p = 12;
        for (int i = 0; i < 10000; i++) {
            String val = i + "";
            long hashcode = XxHash64.hashString(val, 0, val.length(), 0);
            final int idx = (int) (hashcode >>> (64 - p));
            final long w = hashcode << p | 1 << (p - 1);
            final int leading = Long.numberOfLeadingZeros(w) + 1;


            long hashcode2 =  ((long) idx << (64 - p) | (1L << (64 - p - leading )) ) ;
            final int idx2 = (int) (hashcode2 >>> (64 - p));
            final long w2 = hashcode2 << p | 1 << (p - 1);
            final int leading2 = Long.numberOfLeadingZeros(w2) + 1;

            if(leading > 10){
                System.out.println(idx + ":" +idx2 + "," + leading + ":" +leading2 );
            }

            assert (idx == idx2 && leading == leading2);
        }

    }

    @Test
    public void testSquash20() {
        int p = 12;
        int m = 1 << p;
        final int idx = 3;
        final int leading = 5;


        long hashcode2 = ((long) idx << (64 - p) | (1L << (64 - p - leading )) ) ;
        final int idx2 = (int) (hashcode2 >> (64 - p));
        final long w2 = hashcode2 << p | 1 << (p - 1);
        final int leading2 = Long.numberOfLeadingZeros(w2) + 1;

        System.out.println(idx + ":" +idx2 + "," + leading + ":" +leading2 );
    }

    @Test
    public void testSquash() {
        int p = 12;
        int m = 1 << p;
        for (int i = 0; i < 100; i++) {
            String val = i + "";
            long hashcode = XxHash64.hashString(val, 0, val.length(), 0);
            final int idx = (int) (hashcode & (m - 1));
            final long w = hashcode >>> p;
            // Determine the count of leading zeros
            final int leading = Long.numberOfTrailingZeros(w) + 1;


            long hashcode2 = (1L << (p + leading - 1)) | idx;
            final int idx2 = (int) (hashcode2 & (m - 1));
            final long w2 = hashcode2 >>> p;
            // Determine the count of leading zeros
            final int leading2 = Long.numberOfTrailingZeros(w2) + 1;

            System.out.println(idx + ":" +idx2 + "," + leading + ":" +leading2 );
            assert (idx == idx2 && leading == leading2);
        }

    }
}
