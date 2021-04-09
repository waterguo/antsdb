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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;

import com.antsdb.saltedfish.charset.Utf8;

public final class Unicode16 extends BrutalMemoryObject {
    public final static int HEADER_SIZE = 4;
    
    private int length;
    
    public final static class Scanner implements IntSupplier {
        long p;
        long pEnd;
        
        Scanner(long addr, int length) {
            this.p = addr;
            this.pEnd = addr + length * 2;
        }
        
        @Override
        public int getAsInt() {
            if (p >= pEnd) {
                return -1;
            }
            int ch = Unsafe.getShort(p) & 0xffff;
            p += 2;
            return ch;
        }
    }

    public Unicode16(long addr) {
        super(addr);
        int format = Unsafe.getByte(addr);
        if (format != Value.FORMAT_UNICODE16) {
            throw new IllegalArgumentException();
        }
        this.length = Unsafe.getInt3(addr+1);
    }

    public IntSupplier scan() {
        return new Scanner(this.addr + HEADER_SIZE, this.length);
    }
    
    public final static String get(Heap heap, long addr) {
        int format = Unsafe.getByte(addr);
        if (format != Value.FORMAT_UNICODE16) {
            throw new IllegalArgumentException();
        }
        int length = getLength(format, addr);
        char[] chars = new char[length];
        for (int i=0; i<length; i++) {
            char ch = (char)Unsafe.getShort(addr + HEADER_SIZE + i*2);
            chars[i] = ch;
        }
        return new String(chars);
    }
    
    public final static long allocSet(Heap heap, int len) {
        long addr = heap.alloc(HEADER_SIZE + len * 2);
        Unsafe.putByte(addr, Value.FORMAT_UNICODE16);
        Unsafe.putInt3(addr+1, len);
        return addr;
    }
    
    public final static long allocSet(Heap heap, String s) {
        long addr = heap.alloc(HEADER_SIZE + s.length() * 2);
        Unsafe.putByte(addr, Value.FORMAT_UNICODE16);
        Unsafe.putInt3(addr+1, s.length());
        for (int i=0; i<s.length(); i++) {
            Unsafe.putShort(addr + HEADER_SIZE + i*2, (short)s.charAt(i));
        }
        return addr;
    }

    public final static void set(long addr, String s) {
        Unsafe.putByte(addr, Value.FORMAT_UNICODE16);
        Unsafe.putInt3(addr+1, s.length());
        for (int i=0; i<s.length(); i++) {
            Unsafe.putShort(addr + HEADER_SIZE + i*2, (short)s.charAt(i));
        }
    }
    
    public final static boolean isEmpty(long addr) {
        int length = Unsafe.getInt(addr+1);
        return length == 0;
    }
    
    public final static int getLength(int format, long addr) {
        int length = Unsafe.getInt3(addr+1);
        return length;
    }

    public final static int getSize(int format, long addr) {
        int size = getLength(format, addr);
        size = size * 2 + HEADER_SIZE; 
        return size;
    }
    
    public final static char getCharAt(int format, int length, long addr, int idx) {
        if (idx >= length) {
            throw new IllegalArgumentException();
        }
        char ch = (char) Unsafe.getShort(addr + HEADER_SIZE + idx * 2); 
        return ch;
    }

    public final static long concat(Heap heap, byte formatX, long addrX, byte formatY, long addrY) {
        int lengthX = Unicode16.getLength(formatX, addrX);
        int lengthY = Unicode16.getLength(formatY, addrY);
        int length = lengthX + lengthY;
        long addr = heap.alloc(HEADER_SIZE + length * 2);
        Unsafe.putByte(addr, Value.FORMAT_UNICODE16);
        Unsafe.putInt3(addr+1, length);
        Unsafe.copyMemory(addrX + 4, addr + HEADER_SIZE, lengthX * 2);
        Unsafe.copyMemory(addrY + 4, addr + HEADER_SIZE + lengthX * 2, lengthY * 2);
        return addr;
    }

    final static long toBytes(Heap heap, byte format, long addr) {
        int length = getLength(format, addr);
        return Bytes.allocSet(heap, addr + HEADER_SIZE, length * 2);
    }

    public static int compare(long xAddr, long yAddr) {
        int xLength = Unicode16.getLength(Value.FORMAT_UNICODE16, xAddr);
        int yLength = Unicode16.getLength(Value.FORMAT_UNICODE16, yAddr);
        int minLength = Math.min(xLength, yLength);
        for (int i=0; i<minLength; i++) {
            char chx = Unicode16.getCharAt(Value.FORMAT_UNICODE16, xLength, xAddr, i);
            char chy = Unicode16.getCharAt(Value.FORMAT_UNICODE16, yLength, yAddr, i);
            int result = chx - chy;
            if (result != 0) {
                return result;
            }
        }
        for (int i=minLength; i<xLength; i++) {
            char chx = Unicode16.getCharAt(Value.FORMAT_UNICODE16, xLength, xAddr, i);
            if (!Character.isWhitespace(chx)) {
                return 1;
            }
        }
        for (int i=minLength; i<yLength; i++) {
            char chy = Unicode16.getCharAt(Value.FORMAT_UNICODE16, yLength, yAddr, i);
            if (!Character.isWhitespace(chy)) {
                return -1;
            }
        }
        return 0;
    }

    public static long toUtf8(Heap heap, long pValue) {
        int length = getLength(Value.FORMAT_UNICODE16, pValue);
        long pResult = FishUtf8.alloc(heap, length * 3);
        AtomicInteger size = new AtomicInteger();
        for (int i=0;i <length; i++) {
            int ch = Unsafe.getShort(pValue + HEADER_SIZE + i * 2);
            Utf8.encode(ch, byte_ -> {
                long p = pResult + FishUtf8.HEADER_SIZE + size.getAndIncrement();
                Unsafe.putByte(p, (byte)byte_);
            });
        }
        Unsafe.putInt3(pResult+1, size.get());
        return pResult;
    }

    public static void set(long pResult, int idx, char value) {
        long p = pResult + HEADER_SIZE + idx * 2;
        Unsafe.putShort(p, (short)value);
    }

    public static void resize(long addr, int value) {
        int len = getLength(Value.FORMAT_UNICODE16, addr);
        if (value > len) {
            throw new IllegalArgumentException();
        }
        Unsafe.putInt3(addr+1, value);
    }

    public int getLength() {
        return this.length;
    }

    public void resize(int value) {
        if (value > this.length) {
            throw new IllegalArgumentException();
        }
        if (value < this.length) {
            Unsafe.putInt3(this.addr+1, value);
        }
    }

    public void put(int i, char ch) {
        Unsafe.putShort(this.addr + HEADER_SIZE + i * 2, (short)ch);
    }

    @Override
    public int getByteSize() {
        return getSize(Value.FORMAT_UNICODE16, this.addr);
    }

    @Override
    public int getFormat() {
        return Value.FORMAT_UNICODE16;
    }

}
