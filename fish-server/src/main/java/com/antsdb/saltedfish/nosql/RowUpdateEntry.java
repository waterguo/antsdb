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

import com.antsdb.saltedfish.cpp.Unsafe;

/**
 * 
 * @author *-xguo0<@
 */
public class RowUpdateEntry extends LogEntry {
    protected final static int OFFSET_TABLE_ID = 6;
    
    RowUpdateEntry(SpaceManager sm, int rowsize, int tableId) {
        super(sm, 4 + rowsize);
        setTableId(tableId);
    }
    
    protected RowUpdateEntry(long sp, long addr) {
        super(sp, addr);
    }
        
    public static long getHeaderSize() {
        return OFFSET_TABLE_ID + 4;
    }
    
    public long getRowPointer() {
        return this.addr + OFFSET_TABLE_ID + 4;            
    }

    public long getRowSpacePointer() {
        return this.sp + OFFSET_TABLE_ID + 4;
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
        buf.append("sp=");
        buf.append(hex(this.getRowSpacePointer()));
        buf.append(" ");
        buf.append("trxid=");
        buf.append(this.getTrxId());
        return buf.toString();
    }
}

