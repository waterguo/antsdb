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

import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.cpp.Value;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author *-xguo0<@
 */
public final class IndexRow { 
    static Logger _log = UberUtil.getThisLogger();
    protected static final int OFFSET_FORMAT = 0;
    protected static final int OFFSET_NFIELDS = 0x1;
    protected static final int OFFSET_LENGTH = 0x2;
    protected static final int OFFSET_VERSION = 0x4;
    protected static final int OFFSET_ROW_KEY = 0xc;
    protected static final int OFFSET_MISC = OFFSET_ROW_KEY + 1;

    private long addr;
    
    public IndexRow(long addr) {
        this.addr = addr;
    }
    
    public static IndexRow alloc(Heap heap, long version, long pRowKey, byte misc) {
        KeyBytes key = new KeyBytes(pRowKey);
        int keysize = key.getByteSize();
        int size = OFFSET_ROW_KEY + keysize;
        long addr = heap.alloc(size);
        Unsafe.copyMemory(pRowKey, addr + OFFSET_ROW_KEY, keysize);
        Unsafe.putByte(addr + OFFSET_MISC, misc);
        Unsafe.putByte(addr + OFFSET_FORMAT, Value.FORMAT_INDEX_ROW);
        Unsafe.putByte(addr + OFFSET_NFIELDS, (byte)2);
        Unsafe.putShort(addr + OFFSET_LENGTH, (short)size);
        Unsafe.putLong(addr + OFFSET_VERSION, version);
        return new IndexRow(addr);
    }
    
    public int getSize() {
        return Unsafe.getShort(this.addr + OFFSET_LENGTH) & 0xffff;
    }
    
    public long getValueAddress(int field) {
        switch (field) {
        case 0:
            return this.addr + OFFSET_VERSION;
        case 1:
            return this.addr + OFFSET_ROW_KEY;
        case 2:
            return this.addr + OFFSET_MISC;
        default:
            // for future extension
            throw new IllegalArgumentException();
        }
    }
    
    public Object getValue(int field) {
        switch (field) {
        case 0:
            return Unsafe.getLong(this.addr + OFFSET_VERSION);
        case 1:
            return new KeyBytes(this.addr + OFFSET_ROW_KEY);
        case 2:
            return Unsafe.getByte(this.addr + OFFSET_MISC);
        default:
            // for future extension
            throw new IllegalArgumentException();
        }
    }
    
    public long getVersion() {
        return Unsafe.getLong(this.addr + OFFSET_VERSION);
    }
    
    public byte getMisc() {
        return Unsafe.getByte(this.addr + OFFSET_MISC);
    }
    
    public long getRowKeyAddress() {
        return this.addr + OFFSET_ROW_KEY;
    }

    public byte[] getRowKey() {
        return KeyBytes.create(getRowKeyAddress()).get();
    }

    public long getAddress() {
        return this.addr;
    }
    
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("version: ");
        buf.append(getVersion());
        buf.append("\n");
        buf.append("rowkey: ");
        buf.append(KeyBytes.toString(getRowKeyAddress()));
        buf.append("\n");
        buf.append("misc: ");
        buf.append(getMisc());
        return buf.toString();
    }

}
