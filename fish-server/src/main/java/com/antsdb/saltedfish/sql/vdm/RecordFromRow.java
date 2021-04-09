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
import com.antsdb.saltedfish.nosql.Row;

/**
 * 
 * @author *-xguo0<@
 */
public final class RecordFromRow extends RecordBase {
    public RecordFromRow(long addr) {
        super(addr);
        // TODO Auto-generated constructor stub
    }

    @Override
    public long get(int field) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long getKey() {
        // TODO Auto-generated method stub
        return 0;
    }

    public static long get(long pRecord, int field) {
        Row row = Row.fromMemoryPointer(pRecord, 0);
        long pResult;
        if (field == 0) {
            pResult = row.getKeyAddress();
        }
        else {
            pResult = row.getFieldAddress(field - 1);
        }
        return pResult;
    }

    public static long getKey(long pRecord) {
        return Row.getKeyAddress(pRecord);
    }

    public static int size(long pRecord) {
        return Row.fromMemoryPointer(pRecord, 0).getMaxColumnId() + 2;
    }

    public static long clone(Heap heap, long pRecord) {
        int size = Row.getLength(pRecord);
        long pResult = heap.alloc(size);
        Unsafe.copyMemory(pRecord, pResult, size);
        return pResult;
    }
}
