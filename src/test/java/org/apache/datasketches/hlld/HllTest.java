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
    public void testCopy() {
        Hll hll1 = new Hll(14);
        for (int i = 0; i < 1000000; i++) {
            String key = i + "";
            hll1.add(key);
        }

        Hll hll2 = new Hll(12, ByteBuffer.allocate(DirectHllIntArray.getUpdatableSerializationBytes(12)));
        for (int i = 0; i < 1000000; i++) {
            String key = i + "";
            hll2.add(key);
        }

        Hll copy1 = hll1.copy();
        Hll copy2 = hll2.copy();

        System.out.println((long)hll1.size());
        System.out.println((long)hll2.size());
        System.out.println("#####################");
        System.out.println(copy1.hllImpl.getPrecision());
        System.out.println(copy2.hllImpl.getPrecision());
        System.out.println((long)copy1.size());
        System.out.println((long)copy2.size());
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

    @Test
    public void testSer() {
        Hll hll1 = new Hll(14);
        for (int i = 0; i < 1000000; i++) {
            String key = i + "";
            hll1.add(key);
        }

        Hll hll2 = new Hll(12, ByteBuffer.allocate(DirectHllIntArray.getUpdatableSerializationBytes(12)));
        for (int i = 0; i < 1000000; i++) {
            String key = i + "";
            hll2.add(key);
        }

        System.out.println(hll1.size());
        System.out.println(hll2.size());

        byte[] bytes1 = hll1.toBytes();
        byte[] bytes2 = hll2.toBytes();
        Hll hllDer1 = Hll.fromBytes(bytes1);
        Hll hllDer2 = Hll.fromBytes(bytes2);

        System.out.println(hllDer1.size());
        System.out.println(hllDer2.size());
        assert !hllDer1.hllImpl.isMemory();
        assert !hllDer2.hllImpl.isMemory();
    }

    @Test
    public void testWrapBytes() {
        Hll hll1 = new Hll(14);
        for (int i = 0; i < 1000000; i++) {
            String key = i + "";
            hll1.add(key);
        }

        Hll hll2 = new Hll(12, ByteBuffer.allocate(DirectHllIntArray.getUpdatableSerializationBytes(12)));
        for (int i = 0; i < 1000000; i++) {
            String key = i + "";
            hll2.add(key);
        }

        System.out.println(hll1.size());
        System.out.println(hll2.size());

        byte[] bytes1 = hll1.toBytes();
        byte[] bytes2 = hll2.toBytes();

        Hll hllDer1 = Hll.wrapBytes(bytes1);
        Hll hllDer2 = Hll.wrapBytes(bytes2);

        System.out.println(hllDer1.size());
        System.out.println(hllDer2.size());
        assert hllDer1.hllImpl.isMemory();
        assert hllDer2.hllImpl.isMemory();
    }

    @Test
    public void testWrapByteBuffer() {
        Hll hll1 = new Hll(14);
        for (int i = 0; i < 1000000; i++) {
            String key = i + "";
            hll1.add(key);
        }

        Hll hll2 = new Hll(12, ByteBuffer.allocate(DirectHllIntArray.getUpdatableSerializationBytes(12)));
        for (int i = 0; i < 1000000; i++) {
            String key = i + "";
            hll2.add(key);
        }

        System.out.println(hll1.size());
        System.out.println(hll2.size());

        byte[] bytes1 = hll1.toBytes();
        byte[] bytes2 = hll2.toBytes();
        ByteBuffer byteBuffer1 = ByteBuffer.allocate(bytes1.length + 100);
        byteBuffer1.position(100);
        //byteBuffer1.put(bytes1, 100, bytes1.length);
        byteBuffer1.put(bytes1);
        byteBuffer1.position(100);
        ByteBuffer byteBuffer2 = ByteBuffer.allocate(bytes2.length + 200);
        byteBuffer2.position(200);
        //byteBuffer2.put(bytes2, 200, bytes2.length);
        byteBuffer2.put(bytes2);
        byteBuffer2.position(200);

        Hll hllDer1 = Hll.wrapByteBuffer(byteBuffer1);
        Hll hllDer2 = Hll.wrapByteBuffer(byteBuffer2);

        System.out.println(hllDer1.size());
        System.out.println(hllDer2.size());
        assert hllDer1.hllImpl.isMemory();
        assert hllDer2.hllImpl.isMemory();
    }
}
