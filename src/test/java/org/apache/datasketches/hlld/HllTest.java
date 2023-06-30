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

    @Test
    public void testMerge0() {
        // 两个普通Hll, 普通HllUnion
        Hll hll1 = new Hll(14);
        for (int i = 0; i < 1000000; i++) {
            String key = i + "";
            hll1.add(key);
        }

        Hll hll2 = new Hll(12);
        for (int i = 0; i < 1000000; i++) {
            String key = i + "";
            //String key = i + "a";
            hll2.add(key);
        }

        System.out.println((long)hll1.size());
        System.out.println((long)hll2.size());

        HllUnion union = new HllUnion(14);
        union.update(hll1);
        union.update(hll2);

        System.out.println((long) union.getResult().size());
    }

    @Test
    public void testMerge1() {
        // 两个普通Hll, 字节数组HllUnion
        Hll hll1 = new Hll(14);
        for (int i = 0; i < 1000000; i++) {
            String key = i + "";
            hll1.add(key);
        }

        Hll hll2 = new Hll(12);
        for (int i = 0; i < 1000000; i++) {
            //String key = i + "";
            String key = i + "a";
            hll2.add(key);
        }

        System.out.println((long)hll1.size());
        System.out.println((long)hll2.size());

        int p = 14;
        int bytes = DirectHllIntArray.getUpdatableSerializationBytes(p);
        HllUnion union = new HllUnion(p, ByteBuffer.allocate(bytes));
        union.update(hll1);
        union.update(hll2);

        System.out.println((long) union.getResult().size());
        System.out.println(union.getResult().hllImpl.isMemory());
    }

    @Test
    public void testMerge2() {
        // 一个普通Hll一个字节数组Hll, 普通HllUnion
        Hll hll1 = new Hll(14);
        for (int i = 0; i < 1000000; i++) {
            String key = i + "";
            hll1.add(key);
        }

        Hll hll2 = new Hll(12, ByteBuffer.allocate(DirectHllIntArray.getUpdatableSerializationBytes(12)));
        for (int i = 0; i < 1000000; i++) {
            //String key = i + "";
            String key = i + "a";
            hll2.add(key);
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
        // 一个普通Hll一个字节数组Hll, 字节数组HllUnion
        Hll hll1 = new Hll(14);
        for (int i = 0; i < 1000000; i++) {
            String key = i + "";
            hll1.add(key);
        }

        Hll hll2 = new Hll(12, ByteBuffer.allocate(DirectHllIntArray.getUpdatableSerializationBytes(12)));
        for (int i = 0; i < 1000000; i++) {
            //String key = i + "";
            String key = i + "a";
            hll2.add(key);
        }

        System.out.println((long)hll1.size());
        System.out.println((long)hll2.size());

        int p = 14;
        int bytes = DirectHllIntArray.getUpdatableSerializationBytes(p);
        HllUnion union = new HllUnion(p, ByteBuffer.allocate(bytes));
        union.update(hll1);
        union.update(hll2);

        System.out.println((long) union.getResult().size());
        System.out.println(union.getResult().hllImpl.isMemory());
    }

    @Test
    public void testMerge4() {
        // 两个字节数组Hll, 字节数组HllUnion
        Hll hll1 = new Hll(14, ByteBuffer.allocate(DirectHllIntArray.getUpdatableSerializationBytes(14)));
        for (int i = 0; i < 1000000; i++) {
            String key = i + "";
            hll1.add(key);
        }

        Hll hll2 = new Hll(12, ByteBuffer.allocate(DirectHllIntArray.getUpdatableSerializationBytes(12)));
        for (int i = 0; i < 1000000; i++) {
            //String key = i + "";
            String key = i + "a";
            hll2.add(key);
        }

        System.out.println((long)hll1.size());
        System.out.println((long)hll2.size());

        int p = 14;
        int bytes = DirectHllIntArray.getUpdatableSerializationBytes(p);
        HllUnion union = new HllUnion(p, ByteBuffer.allocate(bytes));
        union.update(hll1);
        union.update(hll2);

        System.out.println((long) union.getResult().size());
        System.out.println(union.getResult().hllImpl.isMemory());
    }

    @Test
    public void testMergeBufPosition() {
        // 两个字节数组Hll, 字节数组HllUnion
        int position1 = 100;
        Hll hll1 = new Hll(14, (ByteBuffer)ByteBuffer.allocate(position1 + DirectHllIntArray.getUpdatableSerializationBytes(14)).position(position1));
        for (int i = 0; i < 1000000; i++) {
            String key = i + "";
            hll1.add(key);
        }

        int position2 = 1000;
        Hll hll2 = new Hll(12, (ByteBuffer)ByteBuffer.allocate(position2 + DirectHllIntArray.getUpdatableSerializationBytes(12)).position(position2));
        for (int i = 0; i < 1000000; i++) {
            //String key = i + "";
            String key = i + "a";
            hll2.add(key);
        }

        System.out.println((long)hll1.size());
        System.out.println((long)hll2.size());

        int p = 14;
        int position3 = 2000;
        int bytes = position3 + DirectHllIntArray.getUpdatableSerializationBytes(p);
        HllUnion union = new HllUnion(p, (ByteBuffer)ByteBuffer.allocate(bytes).position(position3));
        union.update(hll1);
        union.update(hll2);

        System.out.println((long) union.getResult().size());
        System.out.println(union.getResult().hllImpl.isMemory());
    }
}
