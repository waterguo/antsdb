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
package com.antsdb.saltedfish.nosql;

import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.cpp.Unsafe;

/**
 * 
 * @author *-xguo0<@
 */
public final class IndexLine {
    private long addr;
    private long rowKey;

    public IndexLine(long pLine) {
        this.addr = pLine;
        this.rowKey = this.addr + 1 + KeyBytes.getRawSize(getKey());
    }
    
    public static IndexLine alloc(Heap heap, byte[] indexKey, byte[] rowKey, byte misc) {
        long pIndexKey = KeyBytes.allocSet(heap, indexKey).getAddress();
        long pRowKey = KeyBytes.allocSet(heap, rowKey).getAddress();
        int indexKeySize = KeyBytes.getRawSize(pIndexKey);
        int rowKeySize = KeyBytes.getRawSize(pRowKey);
        long addr = heap.alloc(1 + indexKeySize + rowKeySize);
        Unsafe.putByte(addr, misc);
        Unsafe.copyMemory(pIndexKey, addr+1, indexKeySize);
        Unsafe.copyMemory(pRowKey, addr+1+indexKeySize, rowKeySize);
        return new IndexLine(addr);
    }
    
    public static IndexLine from(long pLine) {
        if (pLine == 0) {
            return null;
        }
        return new IndexLine(pLine);
    }
    
    public long getKey() {
        return this.addr + 1;
    }
    
    public long getRowKey() {
        return this.rowKey;
    }
    
    public byte getMisc() {
        return Unsafe.getByte(this.addr);
    }

    public long getAddress() {
        return this.addr;
    }

    @Override
    public String toString() {
        return String.format("%s:%s", KeyBytes.toString(getKey()), KeyBytes.toString(getRowKey()));
    }
    
}
