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
public final class TransactionWindowEntry extends LogEntry {
    TransactionWindowEntry(SpaceManager sm, long trxid) {
        super(sm, 8);
        setTrxId(trxid);
        finish(EntryType.TRXWINDOW);
    }
    
    TransactionWindowEntry(long sp, long addr) {
        super(sp, addr);
    }

    /**
     * get the oldest closed transaction at the point of the log position. in another word, after this
     * point all trxid > getTrxid()
     * 
     * @return
     */
    public long getTrxid() {
        return Unsafe.getLong(this.addr + OFFSET_TRX_ID);
    }
    
    void setTrxId(long trxid) {
        Unsafe.putLong(this.addr + OFFSET_TRX_ID, trxid);
    }
}
    
