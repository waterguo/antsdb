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
package com.antsdb.saltedfish.nosql;

import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.cpp.Unsafe;

/**
 * 
 * @author *-xguo0<@
 */
public final class IndexLine {
    static final int OFFSET_VERSION = 0;
    static final int OFFSET_MISC = 8;
    static final int OFFSET_INDEX_KEY = 9;
    private long addr;
    private long rowKey;

    public IndexLine(long pLine) {
        this.addr = pLine;
        this.rowKey = this.addr + OFFSET_INDEX_KEY + KeyBytes.getRawSize(getKey());
    }
    
    public static IndexLine alloc(Heap heap, long version, byte[] indexKey, byte[] rowKey, byte misc) {
        long pIndexKey = KeyBytes.allocSet(heap, indexKey).getAddress();
        long pRowKey = KeyBytes.allocSet(heap, rowKey).getAddress();
        int indexKeySize = KeyBytes.getRawSize(pIndexKey);
        int rowKeySize = KeyBytes.getRawSize(pRowKey);
        long addr = heap.alloc(OFFSET_INDEX_KEY + indexKeySize + rowKeySize);
        Unsafe.putLong(addr + OFFSET_VERSION, version);
        Unsafe.putByte(addr + OFFSET_MISC, misc);
        Unsafe.copyMemory(pIndexKey, addr+OFFSET_INDEX_KEY, indexKeySize);
        Unsafe.copyMemory(pRowKey, addr+OFFSET_INDEX_KEY+indexKeySize, rowKeySize);
        return new IndexLine(addr);
    }
    
    public static IndexLine from(long pLine) {
        if (pLine == 0) {
            return null;
        }
        return new IndexLine(pLine);
    }
    
    public long getKey() {
        return this.addr + OFFSET_INDEX_KEY;
    }
    
    public long getRowKey() {
        return this.rowKey;
    }
    
    public byte getMisc() {
        return Unsafe.getByte(this.addr + OFFSET_MISC);
    }

    public long getVersion() {
        return Unsafe.getLong(this.addr + OFFSET_VERSION);
    }
    
    public long getAddress() {
        return this.addr;
    }

    @Override
    public String toString() {
        return String.format("%s:%s", KeyBytes.toString(getKey()), KeyBytes.toString(getRowKey()));
    }

    public int getRawSize() {
        int result = KeyBytes.getRawSize(getKey()) + KeyBytes.getRawSize(getRowKey()) + 1;
        return result;
    }
    
}
