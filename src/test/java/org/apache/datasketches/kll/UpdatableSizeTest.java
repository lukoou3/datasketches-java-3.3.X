package org.apache.datasketches.kll;

import org.apache.datasketches.quantiles.DoublesSketch;
import org.testng.annotations.Test;

public class UpdatableSizeTest {

    @Test
    public void test() {
        for (int i = 0; i < 9; i++) {
            int n = (int) Math.pow(10, i);
            long len = 100000000L * n;
            int sizeBytes = KllDoublesSketch.getMaxSerializedSizeBytes(200, len, true);
            System.out.println(len +  ":" + sizeBytes + ", " + KllDoublesSketch.getMaxSerializedSizeBytes(200, len, false));
        }

        System.out.println("#################");

        for (int i = 0; i < 9; i++) {
            int n = (int) Math.pow(10, i);
            long len = 100000000L * n;
            int sizeBytes = KllDoublesSketch.getMaxSerializedSizeBytes(400, len, true);
            System.out.println(len +  ":" + sizeBytes + ", " + KllDoublesSketch.getMaxSerializedSizeBytes(400, len, false));
        }
    }

    @Test
    public void test2() {
        for (int i = 0; i < 9; i++) {
            int n = (int) Math.pow(10, i);
            long len = 100000000L * n;
            int sizeBytes = DoublesSketch.getUpdatableStorageBytes(128, len);
            System.out.println(len +  ":" + sizeBytes );
        }

        System.out.println("#################");

        for (int i = 0; i < 9; i++) {
            int n = (int) Math.pow(10, i);
            long len = 100000000L * n;
            int sizeBytes = DoublesSketch.getUpdatableStorageBytes(256, len);
            System.out.println(len +  ":" + sizeBytes);
        }

        System.out.println("#################");

        for (int i = 0; i < 9; i++) {
            int n = (int) Math.pow(10, i);
            long len = 100000000L * n;
            int sizeBytes = DoublesSketch.getUpdatableStorageBytes(512, len);
            System.out.println(len +  ":" + sizeBytes);
        }

    }
}
