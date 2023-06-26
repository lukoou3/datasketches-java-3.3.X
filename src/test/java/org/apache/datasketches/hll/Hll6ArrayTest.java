package org.apache.datasketches.hll;


import org.testng.annotations.Test;

import static org.apache.datasketches.ByteArrayUtil.getShortLE;
import static org.apache.datasketches.ByteArrayUtil.putShortLE;
import static org.apache.datasketches.hll.HllUtil.VAL_MASK_6;

public class Hll6ArrayTest {

    @Test
    public void test6BitArray() {
        int k = 12;
        int m = 1 << 12;
        int m6 = ((m * 3) >>> 2);
        System.out.println(k + "," + m + "," + m6);
        byte[] hllByteArr = new byte[m6];
        int[][] values = {
                {0, 32},
                {1, 33},
                {2, 60},
                {3, 40},
                {6, 33},
                {5, 32},
                {4, 20},
                {8, 40},
                {7, 60},
                {9, 20},
        };

        for (int i = 0; i < values.length; i++) {
            put6Bit(hllByteArr, 0, values[i][0], values[i][1]);
        }

        for (int i = 0; i < values.length; i++) {
            int idx = values[i][0];
            int val = values[i][1];
            int v = get6Bit(hllByteArr, 0, idx);
            assert val == v;
            System.out.println(idx + ":" + val + "," + v+ "," + (val == v) );
        }
    }

    @Test
    public void test6BitShift() {
        /**
         * 0,6,4,2 循环24个bit一循环, 就是6和8的最小公倍数
         */
        for (int i = 0; i < 24; i++) {
            int slotNo = i;
            final int startBit = slotNo * 6;
            final int shift = startBit & 0X7;
            System.out.println(slotNo + "\t" + shift);
        }
    }

    // 更新对应索引的值，每6个bit一个值
    //on-heap
    private static final void put6Bit(final byte[] arr, final int offsetBytes, final int slotNo, final int newValue) {
        final int startBit = slotNo * 6;
        final int shift = startBit & 0X7;
        final int byteIdx = (startBit >>> 3) + offsetBytes;
        final int valShifted = (newValue & 0X3F) << shift; // 0X3F = 63
        final int curMasked = getShortLE(arr, byteIdx) & (~(VAL_MASK_6 << shift));
        final short insert = (short) (curMasked | valShifted);
        putShortLE(arr, byteIdx, insert);
    }

    // 获取对应索引的值，每6个bit一个值
    //on-heap
    private static final int get6Bit(final byte[] arr, final int offsetBytes, final int slotNo) {
        final int startBit = slotNo * 6;
        final int shift = startBit & 0X7;
        final int byteIdx = (startBit >>> 3) + offsetBytes;
        return (byte) ((getShortLE(arr, byteIdx) >>> shift) & 0X3F);
    }
}
