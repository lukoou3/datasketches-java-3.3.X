package org.apache.datasketches.hlld;

import java.nio.ByteBuffer;

public class HllIntArray extends HllImpl {


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
    boolean isMemory() {
        return false;
    }

    @Override
    int getPrecision() {
        return p;
    }

    @Override
    void setPrecision(int precision) {
        this.p = precision;
        this.reg = 1 << p;
    }

    @Override
    HllImpl copy() {
        HllIntArray hll = new HllIntArray(p);
        System.arraycopy(regs, 0, hll.regs, 0, regs.length);
        return hll;
    }

    @Override
    byte[] toBytes() {
        int size = getSerializationBytes();
        ByteBuffer byteBuffer = ByteBuffer.allocate(size);
        byteBuffer.put((byte)1); // version
        byteBuffer.put((byte)p);  // p
        for (int i = 0; i < regs.length; i++) {
            byteBuffer.putInt(regs[i]);
        }
        return byteBuffer.array();
    }

    public static HllIntArray fromBytes(byte[] bytes) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        return fromByteBuffer(byteBuffer);
    }

    public static HllIntArray fromByteBuffer(ByteBuffer byteBuffer) {
        int version = byteBuffer.get();
        if(version != 1){
            throw new IllegalArgumentException("Unsupported version:" + version);
        }
        int p = byteBuffer.get();
        HllIntArray hll = new HllIntArray(p);
        int len = hll.regs.length;
        for (int i = 0; i < len; i++) {
            hll.regs[i] = byteBuffer.getInt();
        }
        return hll;
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
