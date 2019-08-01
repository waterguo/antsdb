/*-------------------------------------------------------------------------------------------------
 _______ __   _ _______ _______ ______  ______
 |_____| | \  |    |    |______ |     \ |_____]
 |     | |  \_|    |    ______| |_____/ |_____]

 Copyright (c) 2016, antsdb.com and/or its affiliates. All rights reserved. *-xguo0<@

 This program is free software: you can redistribute it and/or modify it under the terms of the
 GNU GNU Lesser General Public License, version 3, as published by the Free Software Foundation.

 You should have received a copy of the GNU Affero General Public License along with this program.
 If not, see <https://www.gnu.org/licenses/lgpl-3.0.en.html>
-------------------------------------------------------------------------------------------------*/
package com.antsdb.saltedfish.cpp;

import java.nio.ByteBuffer;

import org.slf4j.Logger;

import com.antsdb.saltedfish.sql.vdm.KeyMaker;
import static com.antsdb.saltedfish.util.UberFormatter.*;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author *-xguo0<@
 */
public final class KeyBytes extends BrutalMemoryObject {
    public static final int HEADER_SIZE = 4;
    private static final KeyBytes KEY_MIN;
    private static final KeyBytes KEY_MAX;
    private static final VariableLengthLongComparator _comp = new VariableLengthLongComparator();
    private static final Logger _log = UberUtil.getThisLogger();
    
    ByteBuffer buffer;
    
    static {
        KEY_MIN = KeyBytes.alloc(new byte[] {0});
        KEY_MAX = KeyBytes.alloc(new byte[] {-1});
        _log.debug("KEY_MIN={} KEY_MAX={}", hex(KEY_MIN.getAddress()), hex(KEY_MAX.getAddress()));
    }
    
    public KeyBytes (long addr) {
        super(addr);
        if (Unsafe.getByte(addr) != Value.FORMAT_KEY_BYTES) {
            throw new IllegalMemoryException(addr);
        }
    }
    
    public static KeyBytes create(long addr) {
        if (addr == 0) {
            return null;
        }
        return new KeyBytes(addr);
    }
    
    public static KeyBytes fromLongValue(long value) {
        byte[] bytes = KeyMaker.make(value);
        return alloc(bytes);
    }
    
    public static KeyBytes fromHexDump(String hex) {
        // find length
        
        int length = 0;
        for (int i=0; i<hex.length(); i++) {
            if (hex.charAt(i) == '-') {
                continue;
            }
            length++;
        }
        
        // parse hex numbers
        
        byte[] bytes = new byte[length / 2];
        int j=0;
        for (int i=0; i<hex.length(); i+=2) {
            while (hex.charAt(i) == '-') {
                i++;
            }
            String s = hex.substring(i, i+2);
            byte bt = (byte)Integer.parseInt(s, 16);
            bytes[j++] = bt;
        }
        
        // little endian flip
        
        KeyMaker.flipEndian(bytes);
        return alloc(bytes);
    }
    
    public static KeyBytes alloc(long pKey) {
        if (pKey == 0) {
            return null;
        }
        if (pKey == KEY_MIN.addr) {
            return KEY_MIN;
        }
        if (pKey == KEY_MAX.addr) {
            return KEY_MAX;
        }
        if (Unsafe.getByte(pKey) != Value.FORMAT_KEY_BYTES) {
            throw new IllegalMemoryException(pKey);
        }
        int len = KeyBytes.getRawSize(pKey);
        ByteBuffer buf = ByteBuffer.allocateDirect(len);
        long addr = UberUtil.getAddress(buf);
        Unsafe.copyMemory(pKey, addr, len);
        KeyBytes result = new KeyBytes(addr);
        result.buffer = buf;
        return result;
    }
    
    public static KeyBytes alloc(byte[] bytes) {
        int len = bytes.length;
        if (len >= Short.MAX_VALUE) {
            throw new IllegalArgumentException();
        }
        if (len == 1) {
            // length 1 is special case for max and min value
            ByteBuffer buf = ByteBuffer.allocateDirect(HEADER_SIZE);
            long addr = UberUtil.getAddress(buf);
            Unsafe.putByte(addr, Value.FORMAT_KEY_BYTES);
            if (bytes[0] > 0) {
                throw new IllegalArgumentException();
            }
            Unsafe.putShort(addr+2, bytes[0]);
            KeyBytes result = new KeyBytes(addr);
            result.buffer = buf;
            return result;
        }
        else {
            ByteBuffer buf = ByteBuffer.allocateDirect(len + HEADER_SIZE);
            long addr = UberUtil.getAddress(buf);
            Unsafe.putByte(addr, Value.FORMAT_KEY_BYTES);
            Unsafe.putShort(addr+2, (short)len);
            Unsafe.putBytes(addr + HEADER_SIZE, bytes);
            KeyBytes result = new KeyBytes(addr);
            result.buffer = buf;
            return result;
        }
    }
    
    public static KeyBytes alloc(Heap heap, int len) {
        if (len >= Short.MAX_VALUE) {
            throw new IllegalArgumentException();
        }
        long addr = heap.alloc(len + HEADER_SIZE, false);
        Unsafe.putByte(addr, Value.FORMAT_KEY_BYTES);
        Unsafe.putByte(addr+1, (byte)0);
        Unsafe.putShort(addr+2, (short)len);
        KeyBytes result = new KeyBytes(addr);
        return result;
    }
    
    public static KeyBytes allocSet(Heap heap, byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        KeyBytes result = alloc(heap, bytes.length);
        Unsafe.putBytes(result.addr + 4, bytes);
        return result;
    }

    public static KeyBytes allocSet(Heap heap, long addr, int length) {
        if (addr == 0) {
            return null; 
        }
        KeyBytes result = alloc(heap, length);
        Unsafe.copyMemory(addr, result.addr + HEADER_SIZE, length);
        return result;
    }

    public static KeyBytes allocSet(Heap heap, KeyBytes key) {
        if (key == null) {
            return null;
        }
        int length = key.getLength() + HEADER_SIZE;
        long pResult = heap.alloc(length);
        Unsafe.copyMemory(key.getAddress(), pResult, length);
        return new KeyBytes(pResult);
    }
    
    public long getDataAddress() {
        return this.addr + HEADER_SIZE;
    }
    
    public static int getUnmaskedLength(long addr) {
        int length = Unsafe.getShortVolatile(addr+2);
        return length;
    }
    
    public int getLength() {
        int length = Unsafe.getShortVolatile(addr+2);
        return (length <= 0) ? 0 : length;
    }
    
    public static int getLength(long addr) {
        int length = Unsafe.getShortVolatile(addr+2);
        return (length <= 0) ? 0 : length;
    }
    
    public static int getRawSize(long pValue) {
        return getLength(pValue) + HEADER_SIZE;
    }
    
    public byte[] get() {
        int length = getUnmaskedLength(addr);
        if (length > 0) {
            byte[] bytes = new byte[length];
            Unsafe.getBytes(addr+HEADER_SIZE, bytes);
            return bytes;
        }
        else {
            byte[] bytes = new byte[1];
            bytes[0] = (byte)length;
            return bytes;
        }
    }

    public static byte[] get(long addr) {
        return create(addr).get();
    }
    
    public void set(long addr, int index, byte value) {
        if (index >= getLength()) {
            throw new IllegalArgumentException();
        }
        Unsafe.putByte(addr + HEADER_SIZE + index, value);
    }
    
    public void set(byte[] bytes) {
        if (bytes.length > getLength()) {
            throw new IllegalArgumentException();
        }
        Unsafe.putShort(addr+2, (short)bytes.length);
        Unsafe.putBytes(addr+HEADER_SIZE, bytes);
    }
    
    public int compare(KeyBytes that) {
        return VariableLengthLongComparator.compare_(this.addr, that.addr);
    }

    public String toString() {
        StringBuilder buf = new StringBuilder();
        if (getUnmaskedLength(this.addr) <= 0) {
            return Integer.toHexString(getUnmaskedLength(this.addr) & 0xff);
        }
        for (int i=0; i<this.getLength(); i+=8) {
            long ii = Unsafe.getLong(this.addr + HEADER_SIZE + i);
            if (i > 0) {
                buf.append('-');
            }
            buf.append(String.format("%016x", ii));
        }
        return buf.toString();
    }

    public static String toString(long pKey) {
        if (pKey == 0) {
            return "NULL";
        }
        return create(pKey).toString();
    }

    public void resize(int length) {
        if (length > getLength()) {
            throw new IllegalArgumentException("size can only shrink");
        }
        Unsafe.putShort(this.addr + 2, (short)length);
    }

    /**
     * get the rowid suffix if this is non unique index
     * @return -1 if the rowid suffix doesnt exist
     */
    public long getSuffix() {
        int len = getLength();
        int suffix = Unsafe.getByte(this.addr + 1) & 0xff;
        if (suffix == 0) {
            return 0;
        }
        int pos = suffix >>> 4;
        long result;
        if (pos > 8) {
            long lastLastLong = Unsafe.getLong(this.addr + 4 + len - 16);
            long lastLong = Unsafe.getLong(this.addr + 4 + len - 8);
            int bits = (16 - pos) * 8;
            result = lastLastLong << bits;
            result = result | (lastLong  >>> (64 - bits));
        }
        else {
            long lastLong = Unsafe.getLong(this.addr + 4 + len - 8);
            int bits = (8 - pos) * 8;
            result = lastLong << bits;
        }
        return result;
    }

    public void setSuffixPostion(int posRowid, int sizeRowid) {
        int len = getLength();
        int posFromTail = len - posRowid;
        int suffix = posFromTail << 4 | sizeRowid;
        Unsafe.putByte(this.addr + 1, (byte)suffix);
    }
    
    public void setSuffixByte(byte value) {
        Unsafe.putByte(this.addr + 1, value);
    }
    
    public byte getSuffixByte() {
        return Unsafe.getByte(this.addr + 1);
    }
    
    public static int compare(long px, long py) {
        return VariableLengthLongComparator.compare_(px, py);
    }
    
    public static long getMinKey() {
        return KEY_MIN.addr;
    }
    
    public static long getMaxKey() {
        return KEY_MAX.addr;
    }

    public static KeyComparator getComparator() {
        return _comp;
    }

    @Override
    public int getByteSize() {
        return getRawSize(this.addr);
    }

    @Override
    public int getFormat() {
        return Value.FORMAT_KEY_BYTES;
    }
}
