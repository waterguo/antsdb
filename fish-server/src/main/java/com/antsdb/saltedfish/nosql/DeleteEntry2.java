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

import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.nosql.Gobbler.EntryType;

/**
 * 
 * @author *-xguo0<@
 */
public final class DeleteEntry2 extends LogEntry {
    protected final static int OFFSET_SESSION = 0x12;
    protected final static int OFFSET_KEY = 0x16;

    DeleteEntry2(SpaceManager sm, int sessionId, int tableId, long trxid, long pKey, int length) {
        super(sm, 4 + 4 + 8 + length);
        setSessionId(sessionId);
        setTrxId(trxid);
        setTableId(tableId);
        Unsafe.copyMemory(pKey, getKeyAddress(), length);
        finish(EntryType.DELETE2);
    }

    DeleteEntry2(long sp, long addr) {
        super(sp, addr);
    }

    public int getSessionId() {
        return Unsafe.getInt(this.addr + OFFSET_SESSION);
    }
    
    public void setSessionId(int value) {
        Unsafe.putInt(this.addr + OFFSET_SESSION, value);
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
    
    void setTableId(int tableId) {
        Unsafe.putInt(this.addr + OFFSET_TABLE_ID, tableId);
    }

    public long getKeyAddress() {
        return this.addr + OFFSET_KEY;
    }

    public static long getHeaderSize() {
        return OFFSET_KEY;
    }
}

