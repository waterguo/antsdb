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
public final class RollbackEntry extends LogEntry {
    protected final static int OFFSET_SESSION = OFFSET_TRX_ID + 8;
    
    RollbackEntry(SpaceManager sm, long trxid, int sessionId) {
        super(sm, 8 + 4);
        setTrxId(trxid);
        setSessionId(sessionId);
        finish(EntryType.ROLLBACK);
    }
    
    RollbackEntry(long sp, long addr) {
        super(sp, addr);
    }

    public long getTrxid() {
        return Unsafe.getLong(this.addr + OFFSET_TRX_ID);
    }
    
    void setTrxId(long trxid) {
        Unsafe.putLong(this.addr + OFFSET_TRX_ID, trxid);
    }
    
    public int getSessionId() {
        return Unsafe.getInt(this.addr + OFFSET_SESSION);
    }
    
    void setSessionId(int value) {
        Unsafe.putInt(this.addr + OFFSET_SESSION, value);
    }
}

