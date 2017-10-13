/*-------------------------------------------------------------------------------------------------
 _______ __   _ _______ _______ ______  ______
 |_____| | \  |    |    |______ |     \ |_____]
 |     | |  \_|    |    ______| |_____/ |_____]

 Copyright (c) 2016, antsdb.com and/or its affiliates. All rights reserved. *-xguo0<@

 This program is free software: you can redistribute it and/or modify it under the terms of the
 GNU Affero General Public License, version 3, as published by the Free Software Foundation.

 You should have received a copy of the GNU Affero General Public License along with this program.
 If not, see <https://www.gnu.org/licenses/agpl-3.0.txt>
-------------------------------------------------------------------------------------------------*/
package com.antsdb.saltedfish.cpp;

/**
 * 
 * @author *-xguo0<@
 */
public final class Int4Array {
    static final int HEADER_SIZE = 4;
    
    private long addr;
    private int length;

    public Int4Array(long addr) {
        if (Unsafe.getByte(addr) != Value.FORMAT_INT4_ARRAY) {
            throw new IllegalArgumentException();
        }
        this.addr = addr;
        this.length = Unsafe.getShort(addr + 2) & 0xffff;
    }
    
    private Int4Array() {
    }
    
    public static Int4Array alloc(Heap heap, byte[] value) {
        if (value.length % 4 != 0) {
            throw new IllegalArgumentException();
        }
        Int4Array result = alloc(heap, value.length / 4);
        for (int i=0; i<value.length; i+=4) {
            int n = value[i + 0] & 0xff;
            n = n << 8 | (value[i + 1] & 0xff);
            n = n << 8 | (value[i + 2] & 0xff);
            n = n << 8 | (value[i + 3] & 0xff);
            result.set(i/4, n);
        }
        return result;
    }
    
    public static Int4Array alloc(Heap heap, int[] value) {
        Int4Array result = alloc(heap, value.length);
        for (int i=0; i<value.length; i++) {
            result.set(i, value[i]);
        }
        return result;
    }
    
    public static Int4Array alloc(Heap heap, int length) {
        if (length > 0xffff) {
            throw new IllegalArgumentException();
        }
        long addr = heap.alloc(HEADER_SIZE + length * 4);
        Unsafe.putByte(addr, Value.FORMAT_INT4_ARRAY);
        Unsafe.putShort(addr + 2, (short)length);
        Int4Array result = new Int4Array();
        result.addr = addr;
        result.length = length;
        return result;
    }
    
    public long getAddress() {
        return this.addr;
    }
    
    /**
     * number of bytes taken by this object
     * @return
     */
    public int getSize() {
        return this.length * 4 + HEADER_SIZE;
    }
    
    public int getLength() {
        return this.length;
    }
    
    public void set(int pos, int value) {
        if ((pos < 0) || (pos >= this.length)) {
            throw new IllegalArgumentException();
        }
        Unsafe.putInt(this.addr + HEADER_SIZE + pos * 4, value);
    }
    
    public int get(int pos) {
        if ((pos < 0) || (pos >= this.length)) {
            throw new IllegalArgumentException();
        }
        return Unsafe.getInt(this.addr + HEADER_SIZE + pos * 4);
    }

    private int get_(int pos) {
        return Unsafe.getInt(this.addr + HEADER_SIZE + pos * 4);
    }

    public int[] toArray() {
        int[] result = new int[this.length];
        for (int i=0; i<this.length; i++) {
            result[i] = get_(i);
        }
        return result;
    }

    public byte[] toBytes() {
        byte[] result = new byte[this.length * 4];
        for (int i=0; i<this.length; i++) {
            int ii = get_(i);
            result[i*4+0] = (byte)(ii >>> 24);
            result[i*4+1] = (byte)(ii >>> 16);
            result[i*4+2] = (byte)(ii >>> 8);
            result[i*4+3] = (byte)(ii >>> 0);
        }
        return result;
    }
    
    public String toString() {
        StringBuilder buf = new StringBuilder();
        for (int i=0; i<this.length; i++) {
            buf.append(get_(i));
            buf.append(',');
        }
        buf.deleteCharAt(buf.length()-1);
        return buf.toString();
    }
}
