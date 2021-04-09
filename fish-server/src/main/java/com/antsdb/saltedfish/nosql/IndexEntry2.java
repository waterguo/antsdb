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
import com.antsdb.saltedfish.cpp.Value;
import com.antsdb.saltedfish.nosql.Gobbler.EntryType;

/**
 * 
 * @author *-xguo0<@
 */
public final class IndexEntry2 extends LogEntry {
    protected final static int OFFSET_SESSION = 0x12;
    protected final static int OFFSET_MISC = 0x16;
    protected final static int OFFSET_INDEX_KEY = 0x17;
    
    public static IndexEntry2 alloc(SpaceManager sm, 
                             int sessionId, 
                             int tableId, 
                             long trxid, 
                             long pIndexKey, 
                             long pRowKey, 
                             byte misc) {
        int indexKeySize = KeyBytes.getRawSize(pIndexKey);
        int rowKeySize = (pRowKey != 0) ? KeyBytes.getRawSize(pRowKey) : 1;
        int size = OFFSET_INDEX_KEY - LogEntry.HEADER_SIZE + indexKeySize + rowKeySize;
        IndexEntry2 entry = new IndexEntry2(sm, size);
        entry.setSessionId(sessionId);
        entry.setTableId(tableId);
        entry.setTrxId(trxid);
        entry.setMisc(misc);
        Unsafe.copyMemory(pIndexKey, entry.getIndexKeyAddress(), indexKeySize);
        if (pRowKey != 0) {
            Unsafe.copyMemory(pRowKey, entry.getIndexKeyAddress() + indexKeySize, rowKeySize);
        }
        else {
            Unsafe.putByte(entry.getIndexKeyAddress() + indexKeySize, Value.FORMAT_NULL);
        }
        entry.finish(EntryType.INDEX2);
        return entry;
    }

    public static IndexEntry2 alloc(Heap heap, 
            int sessionId, 
            int tableId, 
            long trxid, 
            long pIndexKey, 
            long pRowKey,
            byte misc) {
        int indexKeySize = KeyBytes.getRawSize(pIndexKey);
        int rowKeySize = (pRowKey != 0) ? KeyBytes.getRawSize(pRowKey) : 1;
        int size = OFFSET_INDEX_KEY - LogEntry.HEADER_SIZE + indexKeySize + rowKeySize;
        IndexEntry2 entry = new IndexEntry2(heap, size);
        entry.setSessionId(sessionId);
        entry.setTableId(tableId);
        entry.setTrxId(trxid);
        entry.setMisc(misc);
        Unsafe.copyMemory(pIndexKey, entry.getIndexKeyAddress(), indexKeySize);
        if (pRowKey != 0) {
            Unsafe.copyMemory(pRowKey, entry.getIndexKeyAddress() + indexKeySize, rowKeySize);
        }
        else {
            Unsafe.putByte(entry.getIndexKeyAddress() + indexKeySize, Value.FORMAT_NULL);
        }
        entry.finish(EntryType.INDEX2);
        return entry;
    }

    private IndexEntry2(SpaceManager sm, int size) {
        super(sm, size);
    }
    
    public IndexEntry2(Heap heap, int size) {
        super(heap, size);
    }

    IndexEntry2(long sp, long addr) {
        super(sp, addr);
    }

    public void setSessionId(int value) {
        Unsafe.putInt(this.addr + OFFSET_SESSION, value);
    }
    
    public int getSessionId() {
        return Unsafe.getInt(this.addr + OFFSET_SESSION);
    }
    
    public static long getHeaderSize() {
        return OFFSET_MISC;
    }
    
    public long getTrxid() {
        return Unsafe.getLong(this.addr + OFFSET_TRX_ID);
    }
    
    void setTrxId(long trxid) {
        Unsafe.putLong(this.addr + OFFSET_TRX_ID, trxid);
    }

    public int getTableId() {
        return Unsafe.getInt(this.addr + OFFSET_TABLE_ID);
    }
    
    void setTableId(int tableid) {
        Unsafe.putInt(this.addr + OFFSET_TABLE_ID, tableid);
    }

    public long getIndexKeyAddress() {
        return this.addr + OFFSET_INDEX_KEY;
    }

    public long getRowKeyAddress() {
        long p = getIndexKeyAddress();
        int indexKeySize = KeyBytes.getRawSize(p);
        long pRowKey = p + indexKeySize;
        if (Value.getFormat(null, pRowKey) == Value.FORMAT_NULL) {
            return 0;
        }
        else {
            return pRowKey;
        }
    }
    
    public byte getMisc() {
        byte value =  Unsafe.getByte(this.addr + OFFSET_MISC);
        return value;
    }
    
    public void setMisc(byte value) {
        Unsafe.putByte(this.addr + OFFSET_MISC, value);
    }
}

