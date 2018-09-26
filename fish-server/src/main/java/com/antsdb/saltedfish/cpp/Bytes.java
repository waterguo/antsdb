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

import com.antsdb.saltedfish.util.BytesUtil;
import com.antsdb.saltedfish.util.UberUtil;

public class Bytes {
    public final static byte[] get(Heap heap, long addr) {
        if (Unsafe.getByte(addr) != Value.FORMAT_BYTES) {
            throw new IllegalArgumentException();
        }
        int length = Unsafe.getInt3(addr+1);
        byte[] bytes = new byte[length];
        Unsafe.getBytes(addr+4, bytes);
        return bytes;
    }

    public final static int getLength(long addr) {
        int length = Unsafe.getInt3(addr+1);
        return length;
    }
    
    public final static int getRawSize(long addr) {
        int length = Unsafe.getInt3(addr+1);
        return length + 4;
    }
    
    public final static void set(long addr, int index, byte value) {
        Unsafe.putByte(addr + 4 + index, value);
    }
    
    public final static long alloc(Heap heap, int len) {
        long addr = heap.alloc(len + 4);
        Unsafe.putByte(addr, Value.FORMAT_BYTES);
        Unsafe.putInt3(addr+1, len);
        return addr;
    }
    
    public final static long allocSet(Heap heap, byte[] bytes) {
        if (bytes == null) {
            return 0;
        }
        long addr = heap.alloc(bytes.length + 4);
        Unsafe.putByte(addr, Value.FORMAT_BYTES);
        Unsafe.putInt3(addr+1, bytes.length);
        Unsafe.putBytes(addr+4, bytes);
        return addr;
    }

    public static void set(long pKey, byte[] bytes) {
        long addr = pKey;
        Unsafe.putByte(addr, Value.FORMAT_BYTES);
        if (bytes == null) {
            Unsafe.putInt3(addr+1, 0);
            return;
        }
        Unsafe.putInt3(addr+1, bytes.length);
        Unsafe.putBytes(addr+4, bytes);
    }
    
    public static long allocSet(Heap heap, long addr, int length) {
        long addrResult = heap.alloc(length + 4);
        Unsafe.putByte(addrResult, Value.FORMAT_BYTES);
        Unsafe.putInt3(addrResult+1, length);
        Unsafe.copyMemory(addr, addrResult + 4, length);
        return addrResult;
    }

    public static long copy(Heap heap, long pBytes) {
        if (Unsafe.getByte(pBytes) != Value.FORMAT_BYTES) {
            throw new IllegalArgumentException();
        }
        int length = Unsafe.getInt3(pBytes+1);
        long pResult = heap.alloc(length + 4);
        Unsafe.copyMemory(pBytes, pResult, length + 4);
        return pResult;
    }
    
    public final static int compare(long xAddr, long yAddr) {
        if (Unsafe.getByte(xAddr) != Value.FORMAT_BYTES) {
            throw new IllegalArgumentException();
        }
        if (Unsafe.getByte(yAddr) != Value.FORMAT_BYTES) {
            throw new IllegalArgumentException();
        }
        return compare_(xAddr, yAddr);
    }
    
    final static int compare_(long xAddr, long yAddr) {
        int xLength = Unsafe.getInt3(xAddr+1);
        int yLength = Unsafe.getInt3(yAddr+1);
        int minLength = Math.min(xLength, yLength);
        for (int i=0; i<minLength; i++) {
            int btx = Unsafe.getByte(xAddr + 4 + i) & 0xff;
            int bty = Unsafe.getByte(yAddr + 4 + i) & 0xff;
            int result = btx - bty;
            if (result != 0) {
                return result;
            }
        }
        if (xLength > minLength) {
            return 1;
        }
        if (yLength > minLength) {
            return -1;
        }
        return 0;
    }

    public final static String toString(long pBytes) {
        String s = BytesUtil.toCompactHex(get(null, pBytes));
        return s;
    }

    public static byte get(long pValue, int pos) {
        return Unsafe.getByte(pValue + 4 + pos);
    }

    public static void set(ByteBuffer buf, long pSourceData, int length) {
        if (buf.remaining() < (length + 4)) {
            throw new IllegalArgumentException();
        }
        long addr = UberUtil.getAddress(buf) + buf.position();
        Unsafe.putByte(addr, Value.FORMAT_BYTES);
        Unsafe.putInt3(addr+1, length);
        Unsafe.copyMemory(pSourceData, addr + 4, length);
    }

}
