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

import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.cpp.Value;

/**
 * 
 * @author *-xguo0<@
 */
public final class RecordMutable extends RecordBase {
    final static int FORMAT=Value.FORMAT_RECORD;
    final static int OFFSET_SIZE = 2;
    final static int OFFSET_KEY = OFFSET_SIZE + 2;
    final static int OFFSET_FIELDS = OFFSET_KEY + 8;
    
    public RecordMutable(long addr) {
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
        int size = size(pRecord);
        if ((field < 0) || (field >=size)) {
            return 0;
        }
        long pField = pRecord + OFFSET_FIELDS + field * 8;
        long pValue = Unsafe.getLong(pField);
        return pValue;
    }

    public final static int size(long pRecord) {
        long pSize = pRecord + OFFSET_SIZE;
        int size = Unsafe.getShort(pSize);
        return size;
    }

    public static long getKey(long pRecord) {
        long pKey = Unsafe.getLong(pRecord + OFFSET_KEY);
        return pKey;
    }

    public static long clone(Heap heap, long pRecord) {
        int nFields = Record.size(pRecord);
        long pResult = Record.alloc(heap, nFields);
        long pKey = Record.getKey(pRecord);
        Record.setKey(pResult, FishObject.clone(heap, pKey));
        for (int i=0; i<nFields; i++) {
            long pValue = Record.getValueAddress(pRecord, i);
            Record.set(pResult, i, FishObject.clone(heap, pValue));
        }
        return pResult;
    }
}
