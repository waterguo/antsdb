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

import static com.antsdb.saltedfish.util.UberFormatter.hex;

import com.antsdb.saltedfish.cpp.BluntHeap;
import com.antsdb.saltedfish.cpp.Unsafe;

/**
 * 
 * @author *-xguo0<@
 */
public class RowUpdateEntry2 extends LogEntry {
    protected final static int OFFSET_TABLE_ID = 6;
    protected final static int OFFSET_SESSION_ID = 0xa;
    protected final static int OFFSET_ROW = 0xe;
    
    RowUpdateEntry2(SpaceManager sm, int sessionId, int rowsize, int tableId) {
        super(sm, 4 + 4 + rowsize);
        setTableId(tableId);
        setSessionId(sessionId);
    }
    
    public RowUpdateEntry2(BluntHeap heap, int sessionId, int rowsize, int tableId) {
        super(heap, 4 + 4 + rowsize);
        setTableId(tableId);
        setSessionId(sessionId);
    }

    protected RowUpdateEntry2(long sp, long addr) {
        super(sp, addr);
    }
        
    public void setSessionId(int value) {
        Unsafe.putInt(this.addr + OFFSET_SESSION_ID, value);
    }
    
    public int getSessionId() {
        return Unsafe.getInt(this.addr + OFFSET_SESSION_ID);
    }
    
    public static long getHeaderSize() {
        return OFFSET_ROW;
    }
    
    public long getRowPointer() {
        return this.addr + OFFSET_ROW;            
    }

    public long getRowSpacePointer() {
        return this.sp + OFFSET_ROW;
    }
    
    public long getTrxId() {
        long pRow = getRowPointer();
        long trxid = Row.getVersion(pRow);
        return trxid;
    }

    public int getTableId() {
        return Unsafe.getInt(this.addr + OFFSET_TABLE_ID);
    }
    
    void setTableId(int tableId) {
        Unsafe.putInt(this.addr + OFFSET_TABLE_ID, tableId);
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("session=");
        buf.append(getSessionId());
        buf.append(" ");
        buf.append("sp=");
        buf.append(hex(this.getRowSpacePointer()));
        buf.append(" ");
        buf.append("trxid=");
        buf.append(this.getTrxId());
        return buf.toString();
    }
}

