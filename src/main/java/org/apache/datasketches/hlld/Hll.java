package org.apache.datasketches.hlld;

import it.unimi.dsi.fastutil.doubles.Double2IntAVLTreeMap;
import it.unimi.dsi.fastutil.doubles.Double2IntSortedMap;
import org.apache.datasketches.hyperloglog.HyperLogLog;
import org.apache.datasketches.memory.internal.XxHash64;

import java.util.Map;

import static org.apache.datasketches.hlld.HllConstants.*;

/**
 * c语言HyperLogLog版本hlld转换为java实现
 * https://github.com/armon/hlld/tree/master
 */
public class Hll {
    static final int DEFAULT_PRECISION = 12;
    static final int HLL_MIN_PRECISION = 4; // 16 registers
    static final int HLL_MAX_PRECISION = 18; // 262,144 registers
    static final int REG_WIDTH = 6; // Bits per register
    static final int REG_PER_WORD = 5; // floor(INT_WIDTH / REG_WIDTH)

    int p; // precision, number of register bits
    int reg; // reg = 2^p
    int[] regs;

    public Hll() {
        this(DEFAULT_PRECISION);
    }

    public Hll(int precision) {
        if (precision > HLL_MAX_PRECISION || precision < HLL_MIN_PRECISION) {
            throw new RuntimeException("precision");
        }
        this.p = precision;
        // Determine how many registers are needed
        this.reg = 1 << p;
        // Get the full words required
        int words = (reg + REG_PER_WORD - 1) / REG_PER_WORD;
        this.regs = new int[words];
    }

    public void addString(String val) {
        long h = XxHash64.hashString(val, 0, val.length(), 0);
        addHash(h);
    }

    private void addHash(long hashcode) {
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
        int word = regs[idx / REG_PER_WORD];
        word = word >>> (REG_WIDTH * (idx % REG_PER_WORD));
        return word & ((1 << REG_WIDTH) - 1);
    }

    public void setRegister(int idx, int val) {
        int i = idx / REG_PER_WORD;
        int word = regs[i];
        // Shift the val into place
        int shift = REG_WIDTH * (idx % REG_PER_WORD);
        val = val << shift;
        int val_mask = ((1 << REG_WIDTH) - 1) << shift;
        // Store the word
        regs[i] = (word & ~val_mask) | val;
    }

    /*public void merge(Hll hll){
        if (p != hll.p) {
            throw new IllegalArgumentException(
                    "HyperLogLog cannot merge a smaller p into a larger one : "
                            + p + " Provided: " + hll.p);
        }
    }*/

    public double size() {
        RawEstAndNumZeros estAndNumZeros = rawEstimate();
        double rawEst = estAndNumZeros.rawEst;
        int numZeros = estAndNumZeros.numZeros;

        // Check if we need to apply bias correction
        if (rawEst <= 5 * reg) {
            //rawEst -= biasEstimate(rawEst);
            rawEst -= estimateBias(rawEst);
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

    private double alpha() {
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


    private RawEstAndNumZeros rawEstimate() {
        double multi = alpha() * reg * reg;

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

    private long estimateBias(double rawEst) {
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

    private double biasEstimate(double rawEst){
        throw new RuntimeException("biasEstimate");
    }

    private long linearCount(int reg, long numZeros) {
        return Math.round(reg * Math.log(reg / ((double) numZeros)));
    }

    public Hll squash(final int p0) {
        if (p0 > p) {
            throw new IllegalArgumentException(
                    "HyperLogLog cannot be be squashed to be bigger. Current: "
                            + p + " Provided: " + p0);
        }

        /*if (p0 == p) {
            return this;
        }*/

        final Hll dest = new Hll(p0);

        // p, 64 - p
        // +-------------|-------------+
        // |1000000000000|000000001xxxx|  (lr=9 + idx=1024)
        // +-------------|-------------+
        //                \
        // +---------------|-----------+
        // |00000000000|01000000001xxxx|  (lr=2 + idx=0)
        // +---------------|-----------+
        for (int idx = 0; idx < reg; idx++) {
            int regVal = getRegister(idx); // this can be a max of 65, never > 127
            if (regVal != 0) {
                dest.addHash(((long) idx << (64 - p) ) | (1L << (64 -p - regVal)));
            }
        }

        return dest;
    }

    static class RawEstAndNumZeros {
        double rawEst;
        int numZeros;

        public RawEstAndNumZeros(double rawEst, int numZeros) {
            this.rawEst = rawEst;
            this.numZeros = numZeros;
        }
    }
}