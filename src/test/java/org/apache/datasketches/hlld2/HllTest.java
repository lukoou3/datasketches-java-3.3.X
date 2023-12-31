package org.apache.datasketches.hlld2;


import org.apache.datasketches.hyperloglog.StringUtils;
import org.apache.datasketches.memory.internal.XxHash64;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.apache.datasketches.ByteArrayUtil.getIntLE;

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
    public void testSize2() {
        long[] ns = new long[]{1, 3, 10};
        for (long n : ns) {
            Hll hll = new Hll(10);
            for (int i = 0; i < n; i++) {
                String key = i + "";
                hll.add(key);
                hll.add(key);
            }
            long estimate = Math.round(hll.size()) ;
            double percentErr = Math.abs(estimate - n) * 100D / n;
            System.out.println("n:" + n + ",estimate:" + estimate+ ",percentErr:" + percentErr);
        }
    }

    @Test
    public void testMergeFromBuffer() {
        Hll hll = new Hll(14);
        for (int i = 0; i < 3000000; i++) {
            String key = i + "";
            hll.add(key);
        }

        long start = System.currentTimeMillis();

        HllUnion union = new HllUnion(14);
        byte[] bytes = union.toBytes();
        ByteBuffer unionBuffer = ByteBuffer.wrap(bytes);
        ByteBuffer byteBuffer = ByteBuffer.wrap(hll.toBytes());
        for (int i = 0; i < 10000; i++) {
            union = HllUnion.fromByteBuffer(unionBuffer.duplicate());
            ByteBuffer duplicate = byteBuffer.duplicate();
            union.update(Hll.fromByteBuffer(duplicate));
            unionBuffer.duplicate().put(union.toBytes());
        }
        union = HllUnion.fromByteBuffer(unionBuffer.duplicate());
        System.out.println((long)union.getResult().size());
        long end = System.currentTimeMillis();
        System.out.println("ts:" + (end - start) );
    }


    @Test
    public void testMerge0() {
        Hll hll1 = new Hll(12);
        for (int i = 0; i < 10000; i++) {
            String key = i + "";
            hll1.add(key);
        }

        Hll hll2 = new Hll(14);
        for (int i = 0; i < 10000; i++) {
            String key = i + "b ver";
            hll2.add(key);
        }

        System.out.println((long)hll1.size());
        System.out.println((long)hll2.size());

        HllUnion union = new HllUnion(12);
        union.update(hll1);
        union.update(hll2);

        System.out.println((long) union.getResult().size());

    }

    @Test
    public void testMerge() {
        Hll hll1 = new Hll(12);
        for (int i = 0; i < 1000000; i++) {
            String key = i + "";
            hll1.add(key);
            hll1.add(key);
        }

        Hll hll2 = new Hll(14);
        for (int i = 0; i < 1000000; i++) {
            //String key = i + "" ;
            String key = i + "" + "a";
            hll2.add(key);
            hll2.add(key);
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
            hll1.add(key);
            hll1.add(key);
        }

        Hll hll2 = new Hll(12);
        for (int i = 0; i < 10000000; i++) {
            //String key = i + "" ;
            String key = i + "" + "a";
            hll2.add(key);
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
        Hll hll1 = new Hll(14);
        for (int i = 0; i < 10000000; i++) {
            String key = i + "";
            hll1.add(key);
            hll1.add(key);
        }

        Hll hll2 = new Hll(13);
        for (int i = 0; i < 10000000; i++) {
            //String key = i + "" ;
            String key = i + "" + "a";
            hll2.add(key);
            hll2.add(key);
        }

        Hll hll3 = new Hll(13);
        for (int i = 0; i < 10000000; i++) {
            //String key = i + "" ;
            String key = i  + "b";
            hll3.add(key);
            hll3.add(key);
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
    public void testSer() {
        Hll hll = new Hll(12);
        for (int i = 0; i < 10000000; i++) {
            String key = i + "";
            hll.add(key);
        }

        byte[] bytes = hll.toBytes();
        Hll hll2 = Hll.fromBytes(bytes);
        System.out.println(hll.size());
        System.out.println(hll2.size());
        System.out.println(Arrays.equals(hll.regs, hll2.regs) );
    }

    @Test
    public void testSerBase64() {
        Hll hll = new Hll(12);
        for (int i = 0; i < 10000000; i++) {
            String key = i + "";
            hll.add(key);
        }

        String base64Str = hll.toBase64String();
        System.out.println(base64Str);
        System.out.println(hll.regs.length * 4);
        System.out.println(Hll.getSerializationBytes(12));
        System.out.println(base64Str.length());
        Hll hll2 = Hll.fromBase64String(base64Str);
        System.out.println(hll.size());
        System.out.println(hll2.size());
        System.out.println(Arrays.equals(hll.regs, hll2.regs) );
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

    @Test
    public void decodeFromC6bitIntArray2() {
        // 可以解析，大小端要保持一致
        String str = "CgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAA";
        byte[] bytes = StringUtils.decodeBase64(str.getBytes(StandardCharsets.UTF_8));
        System.out.println(bytes.length);
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        int p = byteBuffer.get();
        byteBuffer.get();
        byteBuffer.get();
        byteBuffer.get();
        byteBuffer.get();
        byteBuffer.get();
        byteBuffer.get();
        byteBuffer.get();
        int reg = 1 << p;
        System.out.println(p);
        System.out.println(reg);
        System.out.println(1 + 4 * ( (reg + 5- 1)/ 5 ) );
        Hll hll = new Hll(p);
        int len = hll.regs.length;
        for (int i = 0; i < len; i++) {
            int val = byteBuffer.getInt();
            if(val > 0){
                System.out.println(i + ":" + val);
            }

            hll.regs[i] = val;
        }
        System.out.println(hll.size());

        System.out.println("###############");
    }

    @Test
    public void decodeFromC6bitIntArray3() {
        // 可以解析，大小端要保持一致
        String str = "yVAUBAJCHATEMBgFA0IUA8JAEAQGIRQFQzEQBQNBEAjEMRQGhkAUA8RBFAXDQBAHwkAUAYhhFAQEUQwFhDEQBIkxCATFQAwDRGEUA4YwCAJHUgwExDAMBcZBEAQDYRAHRnEgBEVBFAQEgRgGxkAMBIUyDAXDQBgFA2EQBMQwFAWFMRAFAoEMBwZhIALGQAwDRIEMBghBEATDIBAEhkAIAwpRFATDQhAJw2AUA8NAEARDQRAFREEQBANhFAaDQRADx0AMA4NxEAQEURAFg0MQBAkxIAXKQBgDyIAQBkYxDARIIRQGBFEMBMNBEAJCUQgFxzEkBsRxGAZEcQQCQzEcB8YwGAMIMRgGA2IUAcMRCASCIhAEylAUBMNgEAQHURQChEEQA4RhDAXEcAwIQ0EQBcVBFAgHIRAFRSEIBoRgDAJGIAwEh0AkBAhCEASEQRQEBFEYA4SBEAWIQAwDRWEMB4dQEAeGcAwFBDEIC4tACAQFMRQERFIQBANhGAIEQSADxFAQAwNREAQEQRgMxWAMBcVAEAQGQRAFRjIECIUxEAdDYRAEBkIUBMNAHAMFcRgDRjAUB8JyDAKEMSADA4EQA4NBEAmDQBQGA0EUAwYxKAUDMSAFRzEUA8VgEAREQRQFwmAUBcNgIANFQRwGBWEYBgNRHAQFYRAFBEEcBcNRIAOHQBQIA0IQBkZBDAMDMhQDBWEMBsOgEAQDYRgDw0AMA0ZBHAUHowwFhWEUBAYxEASEMSgCynEYBEUyKAiEURgBBEEUAwdBCAOFIRQCBjEMBUNSJAOHQQgFA2EYBUQxDAVFQRgIwkAQAwMjFAYEURgEhEEQB8UwFANIYSQJBVEcB0MhFAfDMgwHRnEYAsRhGAhFoQwDxXAQBkRhDAQGQRAFRCEQBIZhEAVCUhQJhSEMAoQhDAQFYQwHBGEIBsQgHAdEghQJg2EUBIJRFASFQBAExEEMA0UhHAuFMAwIRHEgBIUhCAPFMBADRDEYA0UxGARDURgEwzAUAwVBFAeGURACBDEUBYNBCAfIURAGBEEUAwRCEAUGMQwFh1EgBgVhHAREQRQEhWAQA0RBGASIYRQEiiEgAAAAAAAAAAAA";
        byte[] bytes = StringUtils.decodeBase64(str.getBytes(StandardCharsets.UTF_8));
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        int p = byteBuffer.get();
        int reg = 1 << p;
        System.out.println(p);
        System.out.println(reg);
        System.out.println(1 + 4 * ( (reg + 5- 1)/ 5 ) );
        Hll hll = new Hll(p);
        int len = hll.regs.length;
        for (int i = 0; i < len; i++) {
            int val = byteBuffer.getInt();
            if(val > 0){
                System.out.println(i + ":" + val);
            }

            hll.regs[i] = val;
        }
        System.out.println(hll.size());

        System.out.println("###############");
    }

    @Test
    public void decodeFromC6bitIntArray4() {
        // 可以解析，大小端要保持一致
        String str = "yVAUBAJCHATEMBgFA0IUA8JAEAQGIRQFQzEQBQNBEAjEMRQGhkAUA8RBFAXDQBAHwkAUAYhhFAQEUQwFhDEQBIkxCATFQAwDRGEUA4YwCAJHUgwExDAMBcZBEAQDYRAHRnEgBEVBFAQEgRgGxkAMBIUyDAXDQBgFA2EQBMQwFAWFMRAFAoEMBwZhIALGQAwDRIEMBghBEATDIBAEhkAIAwpRFATDQhAJw2AUA8NAEARDQRAFREEQBANhFAaDQRADx0AMA4NxEAQEURAFg0MQBAkxIAXKQBgDyIAQBkYxDARIIRQGBFEMBMNBEAJCUQgFxzEkBsRxGAZEcQQCQzEcB8YwGAMIMRgGA2IUAcMRCASCIhAEylAUBMNgEAQHURQChEEQA4RhDAXEcAwIQ0EQBcVBFAgHIRAFRSEIBoRgDAJGIAwEh0AkBAhCEASEQRQEBFEYA4SBEAWIQAwDRWEMB4dQEAeGcAwFBDEIC4tACAQFMRQERFIQBANhGAIEQSADxFAQAwNREAQEQRgMxWAMBcVAEAQGQRAFRjIECIUxEAdDYRAEBkIUBMNAHAMFcRgDRjAUB8JyDAKEMSADA4EQA4NBEAmDQBQGA0EUAwYxKAUDMSAFRzEUA8VgEAREQRQFwmAUBcNgIANFQRwGBWEYBgNRHAQFYRAFBEEcBcNRIAOHQBQIA0IQBkZBDAMDMhQDBWEMBsOgEAQDYRgDw0AMA0ZBHAUHowwFhWEUBAYxEASEMSgCynEYBEUyKAiEURgBBEEUAwdBCAOFIRQCBjEMBUNSJAOHQQgFA2EYBUQxDAVFQRgIwkAQAwMjFAYEURgEhEEQB8UwFANIYSQJBVEcB0MhFAfDMgwHRnEYAsRhGAhFoQwDxXAQBkRhDAQGQRAFRCEQBIZhEAVCUhQJhSEMAoQhDAQFYQwHBGEIBsQgHAdEghQJg2EUBIJRFASFQBAExEEMA0UhHAuFMAwIRHEgBIUhCAPFMBADRDEYA0UxGARDURgEwzAUAwVBFAeGURACBDEUBYNBCAfIURAGBEEUAwRCEAUGMQwFh1EgBgVhHAREQRQEhWAQA0RBGASIYRQEiiEgAAAAAAAAAAAA";
        byte[] bytes = StringUtils.decodeBase64(str.getBytes(StandardCharsets.UTF_8));
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        int p = 10;
        int reg = 1 << p;
        int len = (reg + 5- 1)/ 5;
        Hll hll = new Hll(p);
        for (int i = 0; i < len; i++) {
            //int word =  getIntBE(bytes, i/5 * 4);
            int value =  getIntLE(bytes, i * 4);
            System.out.println(i + ":" + value);
            hll.regs[i] = value;
        }
        System.out.println("###############");
        System.out.println(hll.size());
    }

    @Test
    public void decodeFromC6bitIntArray51() {
        String str1 = "yVAUBAJCHATEMBgFA0IUA8JAEAQGIRQFQzEQBQNBEAjEMRQGhkAUA8RBFAXDQBAHwkAUAYhhFAQEUQwFhDEQBIkxCATFQAwDRGEUA4YwCAJHUgwExDAMBcZBEAQDYRAHRnEgBEVBFAQEgRgGxkAMBIUyDAXDQBgFA2EQBMQwFAWFMRAFAoEMBwZhIALGQAwDRIEMBghBEATDIBAEhkAIAwpRFATDQhAJw2AUA8NAEARDQRAFREEQBANhFAaDQRADx0AMA4NxEAQEURAFg0MQBAkxIAXKQBgDyIAQBkYxDARIIRQGBFEMBMNBEAJCUQgFxzEkBsRxGAZEcQQCQzEcB8YwGAMIMRgGA2IUAcMRCASCIhAEylAUBMNgEAQHURQChEEQA4RhDAXEcAwIQ0EQBcVBFAgHIRAFRSEIBoRgDAJGIAwEh0AkBAhCEASEQRQEBFEYA4SBEAWIQAwDRWEMB4dQEAeGcAwFBDEIC4tACAQFMRQERFIQBANhGAIEQSADxFAQAwNREAQEQRgMxWAMBcVAEAQGQRAFRjIECIUxEAdDYRAEBkIUBMNAHAMFcRgDRjAUB8JyDAKEMSADA4EQA4NBEAmDQBQGA0EUAwYxKAUDMSAFRzEUA8VgEAREQRQFwmAUBcNgIANFQRwGBWEYBgNRHAQFYRAFBEEcBcNRIAOHQBQIA0IQBkZBDAMDMhQDBWEMBsOgEAQDYRgDw0AMA0ZBHAUHowwFhWEUBAYxEASEMSgCynEYBEUyKAiEURgBBEEUAwdBCAOFIRQCBjEMBUNSJAOHQQgFA2EYBUQxDAVFQRgIwkAQAwMjFAYEURgEhEEQB8UwFANIYSQJBVEcB0MhFAfDMgwHRnEYAsRhGAhFoQwDxXAQBkRhDAQGQRAFRCEQBIZhEAVCUhQJhSEMAoQhDAQFYQwHBGEIBsQgHAdEghQJg2EUBIJRFASFQBAExEEMA0UhHAuFMAwIRHEgBIUhCAPFMBADRDEYA0UxGARDURgEwzAUAwVBFAeGURACBDEUBYNBCAfIURAGBEEUAwRCEAUGMQwFh1EgBgVhHAREQRQEhWAQA0RBGASIYRQEiiEgAAAAAAAAAAAA";
        String str2 = "CgAAAAAAAADJUBQEAkIcBMQwGAUDQhQDwkAQBAYhFAVDMRAFA0EQCMQxFAaGQBQDxEEUBcNAEAfCQBQBiGEUBARRDAWEMRAEiTEIBMVADANEYRQDhjAIAkdSDATEMAwFxkEQBANhEAdGcSAERUEUBASBGAbGQAwEhTIMBcNAGAUDYRAExDAUBYUxEAUCgQwHBmEgAsZADANEgQwGCEEQBMMgEASGQAgDClEUBMNCEAnDYBQDw0AQBENBEAVEQRAEA2EUBoNBEAPHQAwDg3EQBARREAWDQxAECTEgBcpAGAPIgBAGRjEMBEghFAYEUQwEw0EQAkJRCAXHMSQGxHEYBkRxBAJDMRwHxjAYAwgxGAYDYhQBwxEIBIIiEATKUBQEw2AQBAdRFAKEQRADhGEMBcRwDAhDQRAFxUEUCAchEAVFIQgGhGAMAkYgDASHQCQECEIQBIRBFAQEURgDhIEQBYhADANFYQwHh1AQB4ZwDAUEMQgLi0AIBAUxFAREUhAEA2EYAgRBIAPEUBADA1EQBARBGAzFYAwFxUAQBAZBEAVGMgQIhTEQB0NhEAQGQhQEw0AcAwVxGANGMBQHwnIMAoQxIAMDgRADg0EQCYNAFAYDQRQDBjEoBQMxIAVHMRQDxWAQBERBFAXCYBQFw2AgA0VBHAYFYRgGA1EcBAVhEAUEQRwFw1EgA4dAFAgDQhAGRkEMAwMyFAMFYQwGw6AQBANhGAPDQAwDRkEcBQejDAWFYRQEBjEQBIQxKALKcRgERTIoCIRRGAEEQRQDB0EIA4UhFAIGMQwFQ1IkA4dBCAUDYRgFRDEMBUVBGAjCQBADAyMUBgRRGASEQRAHxTAUA0hhJAkFURwHQyEUB8MyDAdGcRgCxGEYCEWhDAPFcBAGRGEMBAZBEAVEIRAEhmEQBUJSFAmFIQwChCEMBAVhDAcEYQgGxCAcB0SCFAmDYRQEglEUBIVAEATEQQwDRSEcC4UwDAhEcSAEhSEIA8UwEANEMRgDRTEYBENRGATDMBQDBUEUB4ZREAIEMRQFg0EIB8hREAYEQRQDBEIQBQYxDAWHUSAGBWEcBERBFASFYBADREEYBIhhFASKISAA";
        byte[] bytes1 = StringUtils.decodeBase64(str1.getBytes(StandardCharsets.UTF_8));
        byte[] bytes2 = StringUtils.decodeBase64(str2.getBytes(StandardCharsets.UTF_8));
        System.out.println(11);
    }

    @Test
    public void decodeFromC6bitIntArray5() {
        // 可以解析，大小端要保持一致
        String str = "CgAAAAAAAADJUBQEAkIcBMQwGAUDQhQDwkAQBAYhFAVDMRAFA0EQCMQxFAaGQBQDxEEUBcNAEAfCQBQBiGEUBARRDAWEMRAEiTEIBMVADANEYRQDhjAIAkdSDATEMAwFxkEQBANhEAdGcSAERUEUBASBGAbGQAwEhTIMBcNAGAUDYRAExDAUBYUxEAUCgQwHBmEgAsZADANEgQwGCEEQBMMgEASGQAgDClEUBMNCEAnDYBQDw0AQBENBEAVEQRAEA2EUBoNBEAPHQAwDg3EQBARREAWDQxAECTEgBcpAGAPIgBAGRjEMBEghFAYEUQwEw0EQAkJRCAXHMSQGxHEYBkRxBAJDMRwHxjAYAwgxGAYDYhQBwxEIBIIiEATKUBQEw2AQBAdRFAKEQRADhGEMBcRwDAhDQRAFxUEUCAchEAVFIQgGhGAMAkYgDASHQCQECEIQBIRBFAQEURgDhIEQBYhADANFYQwHh1AQB4ZwDAUEMQgLi0AIBAUxFAREUhAEA2EYAgRBIAPEUBADA1EQBARBGAzFYAwFxUAQBAZBEAVGMgQIhTEQB0NhEAQGQhQEw0AcAwVxGANGMBQHwnIMAoQxIAMDgRADg0EQCYNAFAYDQRQDBjEoBQMxIAVHMRQDxWAQBERBFAXCYBQFw2AgA0VBHAYFYRgGA1EcBAVhEAUEQRwFw1EgA4dAFAgDQhAGRkEMAwMyFAMFYQwGw6AQBANhGAPDQAwDRkEcBQejDAWFYRQEBjEQBIQxKALKcRgERTIoCIRRGAEEQRQDB0EIA4UhFAIGMQwFQ1IkA4dBCAUDYRgFRDEMBUVBGAjCQBADAyMUBgRRGASEQRAHxTAUA0hhJAkFURwHQyEUB8MyDAdGcRgCxGEYCEWhDAPFcBAGRGEMBAZBEAVEIRAEhmEQBUJSFAmFIQwChCEMBAVhDAcEYQgGxCAcB0SCFAmDYRQEglEUBIVAEATEQQwDRSEcC4UwDAhEcSAEhSEIA8UwEANEMRgDRTEYBENRGATDMBQDBUEUB4ZREAIEMRQFg0EIB8hREAYEQRQDBEIQBQYxDAWHUSAGBWEcBERBFASFYBADREEYBIhhFASKISAA";
        byte[] bytes2 = StringUtils.decodeBase64(str.getBytes(StandardCharsets.UTF_8));
        byte[] bytes = new byte[bytes2.length - 8];
        System.arraycopy(bytes2, 8, bytes, 0, bytes2.length - 8);
        int p = 10;
        int reg = 1 << p;
        int len = (reg + 5- 1)/ 5;
        Hll hll = new Hll(p);
        for (int i = 0; i < len; i++) {
            //int word =  getIntBE(bytes, i/5 * 4);
            int value =  getIntLE(bytes, i * 4);
            System.out.println(i + ":" + value);
            hll.regs[i] = value;
        }
        System.out.println("###############");
        System.out.println(hll.size());


        System.out.println("###############");
    }

    @Test
    public void decodeFromCDev() {
        // 可以解析，大小端要保持一致
        String str = "CslQFAQCQhwExDAYBQNCFAPCQBAEBiEUBUMxEAUDQRAIxDEUBoZAFAPEQRQFw0AQB8JAFAGIYRQEBFEMBYQxEASJMQgExUAMA0RhFAOGMAgCR1IMBMQwDAXGQRAEA2EQB0ZxIARFQRQEBIEYBsZADASFMgwFw0AYBQNhEATEMBQFhTEQBQKBDAcGYSACxkAMA0SBDAYIQRAEwyAQBIZACAMKURQEw0IQCcNgFAPDQBAEQ0EQBURBEAQDYRQGg0EQA8dADAODcRAEBFEQBYNDEAQJMSAFykAYA8iAEAZGMQwESCEUBgRRDATDQRACQlEIBccxJAbEcRgGRHEEAkMxHAfGMBgDCDEYBgNiFAHDEQgEgiIQBMpQFATDYBAEB1EUAoRBEAOEYQwFxHAMCENBEAXFQRQIByEQBUUhCAaEYAwCRiAMBIdAJAQIQhAEhEEUBARRGAOEgRAFiEAMA0VhDAeHUBAHhnAMBQQxCAuLQAgEBTEUBERSEAQDYRgCBEEgA8RQEAMDURAEBEEYDMVgDAXFQBAEBkEQBUYyBAiFMRAHQ2EQBAZCFATDQBwDBXEYA0YwFAfCcgwChDEgAwOBEAODQRAJg0AUBgNBFAMGMSgFAzEgBUcxFAPFYBAEREEUBcJgFAXDYCADRUEcBgVhGAYDURwEBWEQBQRBHAXDUSADh0AUCANCEAZGQQwDAzIUAwVhDAbDoBAEA2EYA8NADANGQRwFB6MMBYVhFAQGMRAEhDEoAspxGARFMigIhFEYAQRBFAMHQQgDhSEUAgYxDAVDUiQDh0EIBQNhGAVEMQwFRUEYCMJAEAMDIxQGBFEYBIRBEAfFMBQDSGEkCQVRHAdDIRQHwzIMB0ZxGALEYRgIRaEMA8VwEAZEYQwEBkEQBUQhEASGYRAFQlIUCYUhDAKEIQwEBWEMBwRhCAbEIBwHRIIUCYNhFASCURQEhUAQBMRBDANFIRwLhTAMCERxIASFIQgDxTAQA0QxGANFMRgEQ1EYBMMwFAMFQRQHhlEQAgQxFAWDQQgHyFEQBgRBFAMEQhAFBjEMBYdRIAYFYRwEREEUBIVgEANEQRgEiGEUBIohIAA=";
        byte[] bytes = StringUtils.decodeBase64(str.getBytes(StandardCharsets.UTF_8));
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        int p = byteBuffer.get();
        int reg = 1 << p;
        System.out.println(p);
        System.out.println(reg);
        System.out.println(1 + 4 * ( (reg + 5- 1)/ 5 ) );
        Hll hll = new Hll(p);
        int len = hll.regs.length;
        for (int i = 0; i < len; i++) {
            int val = byteBuffer.getInt();
            /*if(val > 0){
                System.out.println(i + ":" + val);
            }*/

            hll.regs[i] = val;
        }
        System.out.println(hll.size());

        System.out.println("###############");


        System.out.println("###############");
    }



    @Test
    public void decodeFromCDev2() {
        // 可以解析，大小端要保持一致
        String str1 = "CslQFAQCQhwExDAYBQNCFAPCQBAEBiEUBUMxEAUDQRAIxDEUBoZAFAPEQRQFw0AQB8JAFAGIYRQEBFEMBYQxEASJMQgExUAMA0RhFAOGMAgCR1IMBMQwDAXGQRAEA2EQB0ZxIARFQRQEBIEYBsZADASFMgwFw0AYBQNhEATEMBQFhTEQBQKBDAcGYSACxkAMA0SBDAYIQRAEwyAQBIZACAMKURQEw0IQCcNgFAPDQBAEQ0EQBURBEAQDYRQGg0EQA8dADAODcRAEBFEQBYNDEAQJMSAFykAYA8iAEAZGMQwESCEUBgRRDATDQRACQlEIBccxJAbEcRgGRHEEAkMxHAfGMBgDCDEYBgNiFAHDEQgEgiIQBMpQFATDYBAEB1EUAoRBEAOEYQwFxHAMCENBEAXFQRQIByEQBUUhCAaEYAwCRiAMBIdAJAQIQhAEhEEUBARRGAOEgRAFiEAMA0VhDAeHUBAHhnAMBQQxCAuLQAgEBTEUBERSEAQDYRgCBEEgA8RQEAMDURAEBEEYDMVgDAXFQBAEBkEQBUYyBAiFMRAHQ2EQBAZCFATDQBwDBXEYA0YwFAfCcgwChDEgAwOBEAODQRAJg0AUBgNBFAMGMSgFAzEgBUcxFAPFYBAEREEUBcJgFAXDYCADRUEcBgVhGAYDURwEBWEQBQRBHAXDUSADh0AUCANCEAZGQQwDAzIUAwVhDAbDoBAEA2EYA8NADANGQRwFB6MMBYVhFAQGMRAEhDEoAspxGARFMigIhFEYAQRBFAMHQQgDhSEUAgYxDAVDUiQDh0EIBQNhGAVEMQwFRUEYCMJAEAMDIxQGBFEYBIRBEAfFMBQDSGEkCQVRHAdDIRQHwzIMB0ZxGALEYRgIRaEMA8VwEAZEYQwEBkEQBUQhEASGYRAFQlIUCYUhDAKEIQwEBWEMBwRhCAbEIBwHRIIUCYNhFASCURQEhUAQBMRBDANFIRwLhTAMCERxIASFIQgDxTAQA0QxGANFMRgEQ1EYBMMwFAMFQRQHhlEQAgQxFAWDQQgHyFEQBgRBFAMEQhAFBjEMBYdRIAYFYRwEREEUBIVgEANEQRgEiGEUBIohIAA=";
        String str2 = "CoYhGASFYBQDBDEYBoRiEAZEIRwGxFAUB8ZhDAREURQFBWEMBEUxCATDIBADAzEQA4MxIAMEURAFAZEIBcQwDAWEQRQDxTAsBgVBEAiDcggHQ0EcCMZBEAWFUQgDRFIcBgZxGANHQRACRiEIAsJwFAZEMRAExkAUCUlhGAIHQQgHhkAQBENBIASGUQgGAqEYBcdxGAMFQQwHBTEQA0RhEAUHQRwGxEEUA4FBFAVDURQDxDAQBYQgEAUFQQwERUAQBMUwIAaEMhAFQzEUAwVhCAKCURQCRHEQAwRCHAzEQBwHBkEQA4FQEARHUQwChyAQA0XhGAcEkRQDgxIMBINwGAREIRAFA2EMBEQhEAQFMRgExzAMAwhBDAkFoRgDQyEYBMVwDAMCMRgEgzAQAgJSFAXDYBAEREQYBQMhGAYFIRAGyyAYBQNjFAJFURgDBlEUBEpRLAkDYRwGBXEQBUNBDAQCQRAGRlEMBcdRHANHURAFhTEMBgRSFAYJUhQGw3AUBMVBDAVCURQHBEEQAkMxGAZGMRgEBpEYBQVhFAdEciAEQ4EQBUgxFATHQBAGQpEcC0RhFALGIBgEAzEUA8VAEANGQRwCxDAUA4VgEAUEQRgDRFEUA4NgEAJGQRAExCAQCsYwHAbGQSAESjEsAkRAEAOGUQwGBWEQBcZQDAIFIRAJxTAQA8NRFAQCYhwHyUAQBMNgFAwEMQwEBEEMAQJxDASCQAwFQnEQBMYwGAQIQRwGBEEYCERBEAWGUgwHwzAcA0VCEANDQRgERDEQBMdQEAUHURQGwzAUBMRQCAbGISQEwzAUBcRBBAOGQSAEg2AMA8VwDAWEURgEhDEUBYIwCARFMQwIQzEMBcJgDAWFcQwFxWAUBgMhHAUEQgwFSFEMBMZQFANDIQgFiGEMBkNBGAcEQQwExUAUCQRBFATFQRQFBTIMAsRwDAPFIRAFBWIQCQURDAMEcRALxCAcBQJRFALEQBQIyEAUBURBDASCQAwIg1AQBoRAKAyCUAwDREEMBAMxFAMDgRAFh2EcBIIxDARDUhQFhVIUBcJQDAXHUBAEQyEcBUNyFARDMRAEg0EMCMVQEAA=";
        byte[] bytes1 = StringUtils.decodeBase64(str1.getBytes(StandardCharsets.UTF_8));
        byte[] bytes2 = StringUtils.decodeBase64(str2.getBytes(StandardCharsets.UTF_8));
        ByteBuffer byteBuffer1 = ByteBuffer.wrap(bytes1).order(ByteOrder.LITTLE_ENDIAN);
        ByteBuffer byteBuffer2 = ByteBuffer.wrap(bytes2).order(ByteOrder.LITTLE_ENDIAN);
        Hll hll1 = Hll.fromByteBuffer(byteBuffer1);
        Hll hll2 = Hll.fromByteBuffer(byteBuffer2);
        System.out.println(hll1.p);
        System.out.println(hll1.size());
        System.out.println(hll2.p);
        System.out.println(hll2.size());
        HllUnion union = new HllUnion(Math.max(hll1.p, hll2.p));
        union.update(hll1);
        union.update(hll2);
        System.out.println(union.getResult().size());

        System.out.println("###############");
    }

    @Test
    public void decodeFromCDev3() {
        // 可以解析，大小端要保持一致
        String str1 = "DAEBDCEHAgyBQQEIMMQEEGCCBgwxwAEIQQIBCFBDBAAQgwEEEIIBBGFEAQBAggIEIMQGACFBBQgRAAEEMMIDCLBCAQwwwAYIMIMCECACAQRQgwIAYEQDACCCAxAwggMIEMQDDAEBBQQggwIMIEUEGDDCAgwgQQEIIEIBCCDBAwQhgwUAEMIEBEEDAAwwgQEIIQYACCAEAQgSQgEEMcADCDBCBQRRggIIUUEBDDBCBQBgwgEIEMEDCCBABAQgRQIEEUMBCMEAAwghAQYMMEIBBAADBAQgxgIQIIQBBDBEARAAggIEEMIBDEDDBggQBQMUQMMEBGCCBQQBhAIIIEIDEBCBBAgwhwEEcIECEADDAwgAgQEIQQMCCECCAgQRAgQMIEMDCBACBRAQggEEMAEDEDBFAAQgxAIEAAEEDADBAwwQBgcgIIUEDBCCAwQQgAwQEIIBBECCAgRAgwMUEQUEDBDCBQggxAoIMMMBDCACAAQwgAIQMMEGEDFCAQghCwEEIUEDDAAEAhRAQAUMYIEBCECDAwhAAwUIIUQDCDEDARQAgwIIcMICGBCBBQxBBQAUcQABCDDEBghwQwEIEIACFBCDCAwQQwgEIMcFFBCABBRAxQEQMQECCCEAAQhAgQEIIMECADEBBRAxgAIAIQMBCCCEAQQgggIEIYUCCDEBAhwQwgQMIEMCBDCEAAwgyAEIQMICFBCABRgQQgIAEQMFCAFBAgQxBAMQEAICBGDBAgQgBAEAMIQDCBCCAQgQQgQQMEADCDCCAhhAwgEEEIQADEEBAgQwBgEIAMYEFCEEBBAwhwEMEUEDADCAAwQiQgEgEIIAECDEBAgQxQUYAEIDCDBCAggSCAEMEYIFCDDCBwggggkAIIEDCCCBCAQgQgIIEcMBDAHBAhAggwMQMEIDIKBBARhAwQUIQQMBBCBDBQgggAMMQMECCEBFAgwggwMYEMMDBABCBQgQhAEgIcIFBCCCAhwAgQAQIAIABDBBAwRBAgQIUEECCFEDAgAgAQUgEEIBCCCFAAgQQQUEEIUGFJBDBAQQQQIUEQQFFCEFAQwggwIQMIIBDCBCAxgggwEQIMEMGGBBAQgQRAMMIUIBBGCDAhAQBwEEQIIFCGCBAiBAwgEQEMgCCABDAhhAxgEAMEIBDFDGABgRAgUEIEEDECEGAwwAxAEEEMMBCBEDAggQgQMIIQYCADBFAAgwAAIQIQMCBBBCAQwQxgMEEIUDDDCDBQwxAQMIQQUCBCCBBBAgAwMAEIADDAAACAQAhQQEMMIFCBDFBAQQxgIMcMIDDBEEBQgxAgQUIEMDCEFBCQwBAQMMIQIEFBACAQABAwYAIAEDGDDBAgQAQgYEUUMFFFFHAQggxAIEQUAEDEEDAQwwRgIYQQYCBDEEAwhAhAMQEQMGBCBBCBBQwgEIMEECAAACBQBQgQAAAMIBDAFCAhAwgAgEIMAEAJGBABAgggIIUIMBEDDIBwwRgQAUMEMCFCCCARBAAQAQMIECECDBAQgAgwIIIAUFDEDBBBQwQgQIQAIEJDDCAQgQhgIEIIIDCCBBBAgAQgEQIEEBCBCAAgQwwQEAIEIBFBFEAwwQggMYEYEDCEEBAghAggEIIMMECFDCBQxQwwEIMIICBDGCBAARgQIIYEUBADAGAgQgQwUIIMQDCECDAgBAgQMIAEMFECEAAAwgwgMQMAMDGBCBAxwgAwAEIIQBACDAAgQQAQYIEIIADFDFAggQwgEQcQIACCCFBgAQRgUIIEMCACFDBRQwiAQIEAICCAEEAggwgQQEEIACDBECBAwgRQUIQEUBBCBABAQgggUYAAQAACBBAAQQwAAEMIYCCFCDAgAQQQIIAEMDCDIDAxAwxAIAEMUCAEFDABgwRQQMEIIADCDDAxBBQQAIIQQAFBEDAggRAQMQEMECFCFCAQRQggQMAMECDDJCAAwwiQMIAQIBCAIBAgRghQIAQYMEBDCFAwgwhAYAIIAHDFDCBAwQggQEIMECBECDAgAQAQMQIEIDBECEBAgQBAIEIIIACGECARRgQwEEIEABDECCAwwwgwEMIQMBDABAAgwwgQQMIEQBBCCCAwAQhAIYQIICCCEEAxRAAgMIEoEBCBDDARAgRgAEEEMCABDBAQgBgwMIQIMDACCDAwhQhQIEEQQECEBCAhQgBAEIEMoHCGEEAwRgQgEAAYMBAAGCBBQQgwIEgUIBDCCCAgARRAMMAYECECBCAwQhggMEMIMGBCBEAghAgQcQMQIBBECCAQgwQQIIIEQADFBFBAgwQwIIEEMBFDACBABAQwIIMEIADCHEAwQwwAIEMIMFBCFBCQxAgAIUIIQBDBCDAgAwAAQAgIIFBGACAQRggAMcIEICCCDBAxggBAEQIEAFCBBCBAQQgQMIIMEBCCGEAgghwQAIMQECCCBFBCQwQQUMEQICEDCBBQgQwAIMMEIBCABEAhghRAEEMEIFFIBBAQgAwAIMIMEBBABDARBhQQEQQMYDFAEFAwQwwwQMMQMBDBBBAQQwQwUQEMcFGCBBBgRAgwMMAAIECCBABhAwxAYIQMABBDCAAhAxAwMEMEMECADBAQQRAQMQEAEDCGCGAAxQQgMEUAMECCBDAgRQwwYEMQECDABDARAQxQEEQEACCDEDAgwwgQMMUMEBBFBBASAwggIMIIMCEAEDBBAgxAIMIMACCCECBAgQQQQIUIABADDCBQwAQQIMEAIEDGACAwQAQAAQMIUKCCDBBAgwwgEMkYIGDBCBBQghAQEMIEEBDFEKAwBAxAIIMMABFBDCAgxAQQYIQIEGBCBEBAwxAwEEEEQCDABDBBAwAAAIEIIAFCEAAQQwgQIgIEADECGFAQgQgwIQIIEAEIBAARBBAQgIQEQBDDBBAghAggIUEEMBAHBAAwggxAQIAQUBCFDCAgQgwwoUMYUCIEEDBQwQwQYMEMMEBBHDARBAhQYgMEQBDBCHAhBghAEEQMIEBCBDBAQQhAQIEAIABBCBAwyBAgQMIAIFDBCIAhQwhgIQQYIECECDBwQRAwIEIMQCCIACAhARhQQcQEIADDDFAQRBAgMQAAMCAAECAQggggEQMMIDDBBCAgxghQEIQEMABAECAwSQRQQYEIMBACGEAwgwSgUEUAQCCACEAwRAywMIEMMBFHDHBwgggQEMEMUDCHBCBQQQRgQQQIUFCBEFAQQAQwcIIMACBCCBBhAQRAgEQEICDDDEAxAgwgMEEIMDCBBCAgAgwgMIIEMDCDEEAgxAxgQIEYMBDBCABwgRQgQMIEMBDDBDCggQwQMAIIMDDDBDAwhRAQIEIIIODEBEAwhARAAUMIEECBCCAwgwxQIEIMMBAJFDAQQAAwAEQAICCDDFBBBQQAMIMMYBEDGFBQhwwAIMMcICDADDAgwhQgAIUIIBCCFDAwwhAQIAMAECDDDAAwwhQgkMAMECBBBCAQgQggEIIUMDEDAEAhQxBAQMQMEDCBAFBQRRAgIQIYQBBEACAQQxgQMQQEMBICDDAyAQAQMEQEABFEFDCRQQwwMMIQMEEEEDAwwQQgMQAIEEABBCAQQwgQIEoQQBCEADAQggxAEAAMADBIBCAwBwwgEEIAEJAFCCBAQghAMQUMQDBCEDAggAwQcEIEAADHCFAgwwwgAMEcIEEBCDAwAxAQMEEQEDCCACBAwhQgQAIcIECBBCABgQgQIQMIMBECAEBBAQQgYIEUICBFCCARAwQgIQAMACDCECAQgxAgIAMMMCDFECAQxxQwcIEAIBGBEABhAwQgMAEcIEFEBBARAQwgIQMEMCCDCEBAwhAAUMIMIEBDBCAAQghgAMMEMDCCCBAQghgQAEIIUFBEGCAggwAQQEEQECFCBBAwQwQwIAMIECHFCAAQxyQgAAEMMFBBCCAQgQwwYAEYEDBHECAxRBAgYUYEIDCCCCBAhBwwMAEQEFEACCBAgwQAEUIMMCBCCCAgQgwgMQMEIDBGICAQQRwAMMQMICECBEAwhAwwYEQUQDHECDAhAQgwMIQIEGDEBCBQxAwgYMQIUBCCBBAgQQggUIMcQDDCBAAgggQwEIMIABFDBDAgQBQwQMIAQADCECAwwgxAIIcAICDFICAwQRQgQIMEECEADJBAgwgQIIIQECEACCAwQhQQUIEIYDDCDBBRAhwwIQMQECFCECAwgwggMIMYICCCBEBghAggIIYIIHECDCBAQgwgIEIQQBBFDCAggRQAEAAMIEHCCFAwwhBgIEYEQDBGEFAgxBgwIQQQACDFDCAQgQhQMQMAIACCDCBgwwAgIEIIUACCDDBBAxhQMIIIECGBBDAwAQAgEIIMICGECBAgwAgwQMAEYCECDCAwwwwgEAMIAECICIAwBggQIMYIAAAAAC";
        String str2 = "DAEECBAEBwBBgQEIQQIJDFCDAAxAAwMQEMcDBDEDBQggQwIEIIIDBDDDAARARQEQYQIIACCCABAhQQIQIIACCDEAAxxQggUAEEAFBFCEARRRAwEQEMIGDCCEAggwgQMcIUMFCCCEARBAhQIIEAIKBCCAAwQRAgEMIIEBEDCDAQQQwQIEIEMBEECDAQAQxQgMMIMDDDCDBAQgAQQIAAQDBECBAQxQAQEAIEECBCDAAgQQQQIIUMECBCBFBgQghgMEAEAEGBCBBwRQgAEIYgIBDEBCAhABwgMMEMIFBCCBDQhQxQIEUAABHFBCAwwBRwAMQMEBFCCCAQQQQgQEEAQDCADCBAQQQgAIMEIEBDBEAQgwRAEIIYMBBBECAQiRgwEMEQgCEDCDAggRggMYIAMDGCGCCAAgwQIUIYEDBADDAhgRQwMQIQEECCDDAxxQQwEEAYIFBAHEAQhQAgQMIIQECFDDBAwxQwMMMMABCADDAhQgQQcMcEIBDCBDAxBRAQQMIQECFBCEARQggQEUQEAAECBGAwwAxQQIAMIDECCIBAQwAwMIQMQEADBDAggBAwIMUAIBCECAABQAQwEsMQEGEBECAwhQwgMEQMUAACBDAQQwggEIMUQDCBDABAQwwQUgMcEDEDCHBBAgwgQAQQEDFBAFAgQQxQYIMUEBEAIFARAhRAAIMIQCCEDBAwQgggAQMIADBCGBBghgAgIIYMADBECBBAgQhAEMQQQDCACEARBRAgIYIUAAIBFFAQhASAAMUMAFIBBFBBQgwgAEQEICDFBAAggQQwIIQMICEBBDARBBQAIIEMMFBKBJAgggxgUEIEEDBBDCARAQQwEEIEECCDDCAgQgxQIIEUECGFICAgQRAQUEMMIBCBDEAAQxQQEIQEIEBDAAAggwAAQAMQIDCDDDAwhhAQIMEYMBCDBCCxAwQQEoEEIABECDBRghAQQEEMcABCBCAwBAwgIMIEMCECDCAghAQwQIEEQBCBDAAQhAggAMQEMCCIEABAghAgYIIIMBEDBDAQQgAgIkYIUCEBECCAwRggMEQMABDCFDBAQgwQMEQMEABDEDAQQwxAMEQEgCBEDDARAQAgMIAEcFFCADBAhQgAMMAAQBGDACAxQwAwMQQYIDFAEFBAwghAUMIEEGCAGCAggxQQIYUIIEJFKDAQgwggQIsIEBDFCBAwQxSgIMUIQBCAIEBwgiQQAEEEkACDAAAwxwgwgEIQIGEBCBAwyAxQMMMIIBABBDABBAAgAEMIUBABDDAwQhQgQIEEIDCCDDAjAxxAQUQMcCFEFCAgQgQgIcIIIACDCDAggwRQgUcEEAACDBAwQQRAIQQIMADBCBAxQhQwEMQEMCECCBAwwRQgMAYEEEBCDEAAAgxQIgAMEDEGHCABAQhAMUMIIDCDGCAQAwgwEAIIEDCBCIAgwgQwMEMAMEBECAARQghAEEYEUCBCBFAxgQwgMEMEICEEDGAwgwQwIMEIECDCHEAgRQQwIgIIAFFECCAhAQQgMAMEEBADEFAQhAQwAUEIMBCCGDAhQgAQIMEIIBDHBBAgxQQAMEAMIGEEEAAggwgAAcQQEBBBDFAhwghAMYUAYGGECABQzABwYEMQICBEDGAQhQQQQQIQQBDCBABBxAgg8MIIIDCBBDBhBAQQMcEMECACHBAwAAQgIUQAUCBEEGAgggwwEAEEIAECBBBQwhBQEUMQkDABBGAhBAwQIUQMUKBCDBBBAhAQQAMIICCBACAxgQgwMMkMMBEEDCAAxAQwIEEIQFDOBCARAxCQIQEIADBCEIAgwQxgIEkQIDCAHGAhggQQEQEEYBAFCBAgQhAgIIEIMCFBDCAwggwAMIEMIDCDMBAQggQAEEEUQDCFCEAhBQQAIIQIoBHCBCAQghAwMIEEQDDBFDAgQRRQkEEMEFBCEFAwwgAAYMUUMCEABBAxAQxQAMIMIEDCCEASQAwQQIUAMBBCACAwgQQwYEAQIGFDCCAQwRgQAMMAMAHBEDBgAwxwQMcQEBGDECAwQBBgEQIQEEFCACAAgwgwcEMIMFEBBDAQggggIMEEMECDDCAwggwAEMMMICCDFEAwgRQgIIIIIBCCCFAQQgggMIYAMDBFFBAQQgQQIMQMADABECAwwgRAMEYMMCEECCARAAgwIEEMQBHHCCBARAhwEMQMMCECEDAhAARwQAMAQBDBFDBwgwAgIUIMEDBGBCBhgwgwMQIIEDGACCBQwgAQIIMMIDCAEGAwRAQQMQMMIBGCCEARRBhQQIQUIBDCDCAAAhAQMIUIACHBCDABxQxgQMckIDADACABARRQAMMAIBBEACAQwwxAQAUMUFCEHCAgwAhQIQQIEEBFACAAgwAwMEMEQBBBBDAwhBAQIAIMUBBCBCAwwhBAIIIEIFCFDDBRBABAEEEEIBBCDCAxAxRQEIMEIACFDDBhBRAgEEIEQGBCDDAgQAwgIIQEMABECBAhAwQgMMIMIFDCBFAxQwRgEAEEIABCIBARAhAggcIIIDDCADAwgRhAIIMIIBBCABAQxwggEMQYICACBBAhAghQIAEgIBDCEEAwQhAgIIUQIADBEDBQQQhAAIUEEEBGBBAxxQhQMEIEgCBBCCBgiQxAIIMAIADBFCBAgggAIIIEMADDBDBBwRRAUMIEACDBBEARAhCgIEcAMBEECCAwgggwIEUMIBDCEEBAwwAQEQEMQBCBACAgBggwUAIgMDCCBFAwwRwQEMQAMBBGBABAggQwMIIEMFAEBBBgwggwEQMAQAHECDBggggAMQAMQCBFAABAhhQgEcIMcFCDDCAAwgQwgUMIMBEBBBARAwQgMQMMEBCDKCAgwQQgIEMAMBEBAEAAgwgQIEEMIBCFDBAgwRgwIIAAQCBAEAARQSAQIEEEADIDCBABARgAIIMAMCDBEBAQRAwgMIgUAAFCAEAAAhAwMQQIMBCBHEBQSAggEEQIMECBCFAhQQhgIIEIEBDFIDAgQxBgIIYEICCCBCAwhBQQMMIYADADACAQgQwQEAIEEFCADCCBQAhAUMEIMGDBCBBggBRAEMIUEEBCAEBwhQQgEIEEQHCBBDAwgQQgIIIEMGAABCAxARQQQIcUIDDEEEAwQwgQMoEQIDBBCCBAwgggQIcIQBCCCFAAwQhgYEIAUCADDEAggwxQQMMEIECDHFAgwhBAEQgMECBBCEAgAxAgQIQIMBCLDCAgxRAAIIAIYAEBEABRAwxAIAEQEBEHDDAQwQhAEUYIEHBCCCAQgxQgsMIMEFDCBCAwhwhAMAAAICBBBDBAAgxAEIQMYBCGBDAgggggAEMMEAECDCBhARAwMEIEEBBBCDARQQgQEQEQQEFCDEBAwxwgMMIMYADEBBAAhAggQQIcIDBEIDBAAxAgMEcMEADBDAAAQAiAMMAUgBABCHAwATQwMEIMUCEBADAgxBhAMIQUECBABEAgQQRQMMIMEBGGDAAhAwgQMEEMYBCBDBBBQwQwIUkYAEFBEEAgwQxQEEAEMCEBBDAxAwwQIEIQECDEDBAgBAxAMQUMEDDCDBBggxwgQIIIICCBBEAgRwwgMAEEYCDDDEAwQQQAMMUIIBCBCEAQwgwQIIMUADBCBBAwwwQQEAAIIEHAGCAwggwgQQIQYEECBBABAQwAAAEQIDDDBBBgggwQAEQQcCBBFBAgQQggAIEMQDCBHCBAgggwEQMEMCCGAGAxwwgQQIMcgCDDCCAwggQQEMMIMCBDCDBQwQwwEIIEEBCDDEAiQQxgYQEIMABJHGAQgQggEMIAQBCGBCAhRQggIMEQMCEDBCBQRAgwIAEEICBCDCAQiAwQYAUEMAEFBCACQgwwEIEcMCDCDDAABARAMMEQUGCEBDAgwxQgMUMQAECCBCABBCQQEMIIIIACEBAgRQxQEMYMECFBDAAwwQgwEAMEEDCCADARBwggIIMMEEEDBCAQBBhAEQIMYCCEDDAhAwggAMEMMDECFCAQghAgMEEAIBCCCDAAgwQAEIQUEEBBCEAghhgQAEIMEBDEIGABAgggIcEQEADJCCAghAwgIMgMMHBDKCABAhQAEMQMUCGBGCBBgQRAMEEEIDCEHCAAAQgAQIEIMCBDABBARwAAIIMIgECBBGAgwwAQAYQEEEACABAQQwwQQIYIUCEDCEBARQwgAQIIMEECDABQQwggYQEgcDEBCEBwAgwQEMQMUKHDCCAxQAwQgUIUMHDCDDAhQwwwIMQMEBGAEAAQwgQgIIEEMCABDFAQwRhQIMMIUCACFEAQQQhQMQIQEBABGCAwhRQAEQIMQHFDGBBRQgAwIIMUEEABCBAwxQgQIEMcMCDCBAAgxAgAQQAMEBCDEEAwwQwwUgMYUBBECDAgwgggIIIEMAAAAE";
        byte[] bytes1 = StringUtils.decodeBase64(str1.getBytes(StandardCharsets.UTF_8));
        byte[] bytes2 = StringUtils.decodeBase64(str2.getBytes(StandardCharsets.UTF_8));
        ByteBuffer byteBuffer1 = ByteBuffer.wrap(bytes1);
        ByteBuffer byteBuffer2 = ByteBuffer.wrap(bytes2);
        Hll hll1 = Hll.fromByteBuffer(byteBuffer1);
        Hll hll2 = Hll.fromByteBuffer(byteBuffer2);
        System.out.println("p:" + hll1.p);
        System.out.println("size:" + hll1.size());
        System.out.println("p:" + hll2.p);
        System.out.println("size:" + hll2.size());
        HllUnion union = new HllUnion(Math.max(hll1.p, hll2.p));
        union.update(hll1);
        union.update(hll2);
        System.out.println("merge size:" + union.getResult().size());

        System.out.println("###############");
    }
}
