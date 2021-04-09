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
import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.nosql.Gobbler.EntryType;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class LogEntry {
    public final static int HEADER_SIZE = 6;
    protected final static int OFFSET_MAGIC = 0;
    protected final static int OFFSET_ENTRY_TYPE = 2;
    protected final static int OFFSET_SIZE = 3;
    protected final static int OFFSET_TRX_ID = 6;
    protected final static int OFFSET_TABLE_ID = 0xe;
    public static short MAGIC = 0x7777;
    private static EntryType[] _types = EntryType.values(); 
    protected long sp;
    protected long addr;
    
    LogEntry(SpaceManager sm, int size) {
        this.sp = sm.alloc(HEADER_SIZE + size);
        this.addr = sm.toMemory(this.sp);
        setSize(size);
    }
    
    public LogEntry(Heap heap, int size) {
        this.addr = heap.alloc(HEADER_SIZE + size);
        setSize(size);
    }

    public LogEntry(long sp, long addr) {
        this.sp = sp;
        this.addr = addr;
        if (getMagic() != MAGIC) {
            UberUtil.error("invalid log entry at lp={} p={}", sp, addr);
        }
    }
    
    protected void finish(EntryType type) {
        setType(type);
        setMagic();
    }
    
    final short getMagic() {
        return Unsafe.getShortVolatile(addr + OFFSET_MAGIC);
    }
    
    public final void setMagic() {
        Unsafe.putShortVolatile(addr + OFFSET_MAGIC, MAGIC);
    }
    
    public static final LogEntry getEntry(long sp, long p) {
        Gobbler.EntryType type = new LogEntry(sp, p).getType();
        LogEntry result;
        switch (type) {
            case INSERT2:
                result = new InsertEntry2(sp, p);
                break;
            case UPDATE2:
                result = new UpdateEntry2(sp, p);
                break;
            case PUT2:
                result = new PutEntry2(sp, p);
                break;
            case DELETE2:
                result = new DeleteEntry2(sp, p);
                break;
            case DELETE_ROW2:
                result = new DeleteRowEntry2(sp, p);
                break;
            case COMMIT:
                result = new CommitEntry(sp, p);
                break;
            case ROLLBACK:
                result = new RollbackEntry(sp, p);
                break;
            case INDEX2:
                result = new IndexEntry2(sp, p);
                break;
            case MESSAGE:
                result = new MessageEntry(sp, p);
                break;
            case MESSAGE2:
                result = new MessageEntry2(sp, p);
                break;
            case TRXWINDOW:
                result = new TransactionWindowEntry(sp, p);
                break;
            case TIMESTAMP:
                result = new TimestampEntry(sp, p);
                break;
            case DDL:
                result = new DdlEntry(sp, p);
                break;
            default:
                throw new IllegalArgumentException(String.valueOf(type));
        }
        return result;
    }

    public final EntryType getType() {
        byte bt = Unsafe.getByteVolatile(addr + OFFSET_ENTRY_TYPE);
        return _types[bt];
    }
    
    public final static EntryType getType(long addr) {
        byte bt = Unsafe.getByteVolatile(addr + OFFSET_ENTRY_TYPE);
        return _types[bt];
    }
    
    public final void setType(EntryType type) {
        byte bt = (byte)type.ordinal();
        Unsafe.putByteVolatile(addr + OFFSET_ENTRY_TYPE, bt);
    }

    public void setType(byte value) {
        Unsafe.putByteVolatile(addr + OFFSET_ENTRY_TYPE, value);
    }
    
    public final int getSize() {
        return Unsafe.getInt3Volatile(addr + OFFSET_SIZE);
    }
    
    public final void setSize(int size) {
        Unsafe.putInt3Volatile(addr + OFFSET_SIZE, size);
    }

    public final long getAddress() {
        return this.addr;
    }
    
    public final long getSpacePointer() {
        return this.sp;
    }
}
