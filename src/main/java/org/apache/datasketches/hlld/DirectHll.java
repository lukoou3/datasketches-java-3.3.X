package org.apache.datasketches.hlld;

import it.unimi.dsi.fastutil.doubles.Double2IntAVLTreeMap;
import it.unimi.dsi.fastutil.doubles.Double2IntSortedMap;
import org.apache.datasketches.memory.internal.XxHash64;

import java.nio.ByteBuffer;
import java.util.Map;

import static org.apache.datasketches.hlld.Hll.*;
import static org.apache.datasketches.hlld.HllConstants.*;

/**
 * 基于ByteBuffer操作Hll，主要用于druid中聚合使用堆外内存的情况
 */
public class DirectHll {
    /**
     * byte offset:
     * Byte 0: version
     * Byte 1: precision
     * Byte 2: regs
     */
    public static final int PRECISION_BYTE = 1;
    public static final int REGS_BYTE = 2;

    private ByteBuffer byteBuffer;
    private int initPosition;

    public DirectHll(int precision, ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
        this.initPosition = byteBuffer.position();
        this.byteBuffer.put(this.initPosition, (byte)1);
        this.byteBuffer.put(this.initPosition + PRECISION_BYTE, (byte)precision);
    }

    public void add(long val) {
        final long[] data = { val };
        long h = XxHash64.hashLongs(data, 0, data.length, DEFAULT_HASH_SEED);
        addHash(h);
    }

    public void add(double val) {
        final double[] data = { val };
        long h = XxHash64.hashDoubles(data, 0, data.length, DEFAULT_HASH_SEED);
        addHash(h);
    }

    public void add(String val) {
        if(val == null || val.isEmpty()){
            return;
        }
        long h = XxHash64.hashString(val, 0, val.length(), DEFAULT_HASH_SEED);
        addHash(h);
    }

    public void add(byte[] val) {
        long h = XxHash64.hashBytes(val, 0, val.length, DEFAULT_HASH_SEED);
        addHash(h);
    }

    public void addHash(long hashcode) {
        int p = byteBuffer.get(initPosition + PRECISION_BYTE);
        // Determine the index using the first p bits
        final int idx = (int) (hashcode >>> (64 - p));
        // Shift out the index bits
        final long w = hashcode << p | 1 << (p - 1);
        // Determine the count of leading zeros
        final int leading = Long.numberOfLeadingZeros(w) + 1;
        // Update the register if the new value is larger
        if (leading > getRegister(idx)) {
            setRegister(idx, leading);
        }
    }

    public int getRegister(int idx) {
        int word = byteBuffer.getInt(initPosition + REGS_BYTE + ((idx / REG_PER_WORD) << 2) );
        word = word >>> (REG_WIDTH * (idx % REG_PER_WORD));
        return word & ((1 << REG_WIDTH) - 1);
    }

    public void setRegister(int idx, int val) {
        int i = idx / REG_PER_WORD;
        int pos = initPosition + REGS_BYTE + (i << 2);
        int word = byteBuffer.getInt(pos );
        // Shift the val into place
        int shift = REG_WIDTH * (idx % REG_PER_WORD);
        val = val << shift;
        int val_mask = ((1 << REG_WIDTH) - 1) << shift;
        // Store the word
        byteBuffer.putInt(pos, (word & ~val_mask) | val);
    }

    public double getEstimate() {
        return size();
    }

    public double size() {
        int p = byteBuffer.get(initPosition + PRECISION_BYTE);
        int reg = 1 << p;

        RawEstAndNumZeros estAndNumZeros = rawEstimate(p, reg);
        double rawEst = estAndNumZeros.rawEst;
        int numZeros = estAndNumZeros.numZeros;

        // Check if we need to apply bias correction
        if (rawEst <= 5 * reg) {
            //rawEst -= biasEstimate(rawEst);
            rawEst -= estimateBias(rawEst, p);
        }

        // Check if linear counting should be used
        double altEst;
        if (numZeros != 0) {
            altEst = linearCount(reg, numZeros);
        } else {
            altEst = rawEst;
        }

        // Determine which estimate to use
        if (altEst <= thresholdData[p-4]) {
            return altEst;
        } else {
            return rawEst;
        }
    }

    private double alpha(int p, int reg) {
        switch (p) {
            case 4:
                return 0.673;
            case 5:
                return 0.697;
            case 6:
                return 0.709;
            default:
                return 0.7213 / (1 + 1.079 / reg);
        }
    }

    private RawEstAndNumZeros rawEstimate(int p, int reg) {
        double multi = alpha(p, reg) * reg * reg;

        int numZeros = 0;
        double sum = 0;
        int regVal;
        for (int i = 0; i < reg; i++) {
            regVal = getRegister(i);
            sum += inversePow2Data[regVal];
            if (regVal == 0) {
                numZeros++;
            }
        }
        return new RawEstAndNumZeros(multi / sum, numZeros);
    }

    private long estimateBias(double rawEst, int p) {
        double[] rawEstForP = rawEstimateData[p - 4];

        // compute distance and store it in sorted map
        Double2IntSortedMap estIndexMap = new Double2IntAVLTreeMap();
        double distance = 0;
        for (int i = 0; i < rawEstForP.length; i++) {
            distance = Math.pow(rawEst - rawEstForP[i], 2);
            estIndexMap.put(distance, i);
        }

        // take top-k closest neighbors and compute the bias corrected cardinality
        long result = 0;
        double[] biasForP = biasData[p - 4];
        double biasSum = 0;
        int kNeighbors = K_NEAREST_NEIGHBOR;
        for (Map.Entry<Double, Integer> entry : estIndexMap.entrySet()) {
            biasSum += biasForP[entry.getValue()];
            kNeighbors--;
            if (kNeighbors <= 0) {
                break;
            }
        }

        // 0.5 added for rounding off
        result = (long) ((biasSum / K_NEAREST_NEIGHBOR) + 0.5);
        return result;
    }

    private long linearCount(int reg, long numZeros) {
        return Math.round(reg * Math.log(reg / ((double) numZeros)));
    }


}
