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
package com.antsdb.saltedfish.sql.vdm;

import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.Unsafe;

/**
 * 
 * @author *-xguo0<@
 */
public final class GroupContext {
    private static final int OFFSET_NVARIABLES = 0;
    private static final int OFFSET_RECORD = 2;
    private static final int OFFSET_COUNTER = 0xa;
    private static final int OFFSET_VARIABLES = 0x12;
    
    private long addr;
    short nvariables;

    public GroupContext(long addr) {
        this.addr = addr;
        this.nvariables = Unsafe.getShort(addr);
    }
    
    public static GroupContext alloc(Heap heap, int nvariables) {
        long pResult = heap.alloc(OFFSET_VARIABLES + nvariables * 8, true);
        Unsafe.putShort(pResult + OFFSET_NVARIABLES, (short)nvariables);
        Unsafe.putLong(pResult + OFFSET_COUNTER, 0);
        return new GroupContext(pResult);
    }
    
    public long getAddress() {
        return this.addr;
    }
    
    public long getRecord() {
        return Unsafe.getLong(this.addr + OFFSET_RECORD);
    }

    public void setRecord(long pRecord) {
        Unsafe.putLong(this.addr + OFFSET_RECORD, pRecord);
    }
    
    public void count() {
        Unsafe.getAndAddLong(this.addr + OFFSET_COUNTER, 1);
    }
    
    public long getCounter() {
        return Unsafe.getLong(this.addr + OFFSET_COUNTER);
    }
    
    public long getCounterPointer() {
        return this.addr + OFFSET_COUNTER;
    }
    
    public long getVarialbe(int variableId) {
        return Unsafe.getLong(this.addr + OFFSET_VARIABLES + variableId * 8);
    }

    public void setVariable(int variableId, long pValue) {
        Unsafe.putLong(this.addr + OFFSET_VARIABLES + variableId * 8, pValue);
    }
}
