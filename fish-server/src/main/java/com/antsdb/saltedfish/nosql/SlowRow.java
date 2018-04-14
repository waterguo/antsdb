/*-------------------------------------------------------------------------------------------------
 _______ __   _ _______ _______ ______  ______
 |_____| | \  |    |    |______ |     \ |_____]
 |     | |  \_|    |    ______| |_____/ |_____]

 Copyright (c) 2016, antsdb.com and/or its affiliates. All rights reserved. *-xguo0<@

 This program is free software: you can redistribute it and/or modify it under the terms of the
 GNU Affero General Public License, version 3, as published by the Free Software Foundation.

 You should have received a copy of the GNU Affero General Public License along with this program.
 If not, see <https://www.gnu.org/licenses/agpl-3.0.txt>
-------------------------------------------------------------------------------------------------*/
package com.antsdb.saltedfish.nosql;

import java.util.HashMap;

import com.antsdb.saltedfish.cpp.BluntHeap;
import com.antsdb.saltedfish.sql.vdm.KeyMaker;

/**
 * SlowRow is built using the data from Row. It is not using direct memory. It is made for convenience by
 * sacrificing performance.
 * 
 * @author wgu0
 */
public class SlowRow extends HashMap<Integer, Object> {
    private static final long serialVersionUID = 1L;
    
    byte[] key;
    long version = 1;
    int maxColumnId = 0;
    boolean isMutable = true;
    
    private SlowRow() {
    }
    
    public SlowRow(Object... values) {
        if (values.length == 0) {
            throw new IllegalArgumentException();
        }
        else if ((values.length == 1) && (values[0] instanceof byte[])) {
            this.key = (byte[])values[0];
        }
        else {
            this.key = KeyMaker.gen(values);
        }
    }

    public byte[] getKey() {
        return this.key;
    }
    
    public void setKey(byte[] byteArray) {
        if (!this.isMutable) {
            throw new IllegalArgumentException();
        }
        this.key = byteArray;
    }
    
    public void set(Integer index, Object value) {
        if (!this.isMutable) {
            throw new IllegalArgumentException();
        }
        put(index, value);
        this.maxColumnId = Math.max(this.maxColumnId, index);
    }

    @Override
    public SlowRow clone() {
        SlowRow result = new SlowRow(this.key);
        result.maxColumnId = this.maxColumnId;
        result.key = this.key;
        result.version = this.version;
        result.putAll(this);
        result.isMutable = true;
        return result;
    }

    public static SlowRow from(Row row) {
        if (row == null) {
            return null;
        }
        SlowRow result = new SlowRow();
        result.setKey(row.getKey());
        result.setTrxTimestamp(row.getTrxTimestamp());
        for (int i=0; i<=row.maxColumnid; i++) {
            Object value = row.get(i);
            result.set(i, value);
        }
        return result;
    }

    public static SlowRow fromRowPointer(long pRow, long version) {
        if (pRow == 0) {
            return null;
        }
        Row row = Row.fromMemoryPointer(pRow, version);
        return from(row);
    }

    public void setTrxTimestamp(long trxTimestamp) {
        if (!this.isMutable) {
            throw new IllegalArgumentException();
        }
        this.version = trxTimestamp;
    }

    public long getTrxTimestamp() {
        return this.version;
    }

    public VaporizingRow toVaporisingRow(BluntHeap heap) {
        return new VaporizingRow(heap, this);
    }
    
    public int getMaxColumnId() {
        return this.maxColumnId; 
    }

    public void setMutable(boolean value) {
        if (!this.isMutable && value) {
            // making a read-only object mutable is not allowed
            throw new IllegalArgumentException();
        }
        this.isMutable = value;
    }
    
}
