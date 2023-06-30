package org.apache.datasketches.hlld;

import java.nio.ByteBuffer;

import static org.apache.datasketches.hlld.HllIntArray.REG_PER_WORD;
import static org.apache.datasketches.hlld.HllIntArray.REG_WIDTH;


/**
 * 基于ByteBuffer操作Hll，主要用于druid中聚合使用堆外内存的情况
 */
public class DirectHllIntArray extends HllImpl {
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

    DirectHllIntArray(int precision, ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
        this.initPosition = byteBuffer.position();
        int reg = 1 << precision;
        assert byteBuffer.limit() - byteBuffer.position() >= (reg + REG_PER_WORD - 1) / REG_PER_WORD * 4;
        this.byteBuffer.put(this.initPosition, (byte)1);
        this.byteBuffer.put(this.initPosition + PRECISION_BYTE, (byte)precision);
    }

    @Override
    int getRegister(int idx) {
        int word = byteBuffer.getInt(initPosition + REGS_BYTE + ((idx / REG_PER_WORD) << 2) );
        word = word >>> (REG_WIDTH * (idx % REG_PER_WORD));
        return word & ((1 << REG_WIDTH) - 1);
    }

    @Override
    void setRegister(int idx, int val) {
        int i = idx / REG_PER_WORD;
        int pos = initPosition + REGS_BYTE + (i << 2);
        int word = byteBuffer.getInt(pos);
        // Shift the val into place
        int shift = REG_WIDTH * (idx % REG_PER_WORD);
        val = val << shift;
        int val_mask = ((1 << REG_WIDTH) - 1) << shift;
        // Store the word
        byteBuffer.putInt(pos, (word & ~val_mask) | val);
    }

    @Override
    void reset() {
        int p = getPrecision();
        int reg = 1 << p;
        int words = (reg + REG_PER_WORD - 1) / REG_PER_WORD;
        final long endBytes = REGS_BYTE + words << 2;
        for (int i = REGS_BYTE; i < endBytes; i++) {
            byteBuffer.put((byte)0);
        }
    }

    @Override
    int getPrecision() {
        return byteBuffer.get(initPosition + PRECISION_BYTE);
    }

    public static final int getUpdatableSerializationBytes(final int precision){
        int reg = 1 << precision;
        int words = (reg + REG_PER_WORD - 1) / REG_PER_WORD;
        // version + p + regs
        return 1 + 1 + words * 4;
    }
}
