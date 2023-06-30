package org.apache.datasketches.hlld;

public class HllIntArray extends HllImpl {
    static final int REG_WIDTH = 6; // Bits per register
    static final int REG_PER_WORD = 5; // floor(INT_WIDTH / REG_WIDTH)

    int p; // precision, number of register bits
    int reg; // reg = 2^p
    int[] regs;

    HllIntArray(int precision) {
        this.p = precision;
        // Determine how many registers are needed
        this.reg = 1 << p;
        // Get the full words required
        int words = (reg + REG_PER_WORD - 1) / REG_PER_WORD;
        this.regs = new int[words];
    }

    @Override
    int getRegister(int idx) {
        int word = regs[idx / REG_PER_WORD];
        word = word >>> (REG_WIDTH * (idx % REG_PER_WORD));
        return word & ((1 << REG_WIDTH) - 1);
    }

    @Override
    void setRegister(int idx, int val) {
        int i = idx / REG_PER_WORD;
        int word = regs[i];
        // Shift the val into place
        int shift = REG_WIDTH * (idx % REG_PER_WORD);
        val = val << shift;
        int val_mask = ((1 << REG_WIDTH) - 1) << shift;
        // Store the word
        regs[i] = (word & ~val_mask) | val;
    }

    @Override
    void reset() {
        for (int i = 0; i < reg; i++) {
            regs[i] = 0;
        }
    }

    @Override
    int getPrecision() {
        return p;
    }

    public int getP() {
        return p;
    }

    public void setP(int p) {
        this.p = p;
    }

    public int getReg() {
        return reg;
    }

    public void setReg(int reg) {
        this.reg = reg;
    }

    public int[] getRegs() {
        return regs;
    }

    public void setRegs(int[] regs) {
        this.regs = regs;
    }
}
