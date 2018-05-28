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

import org.apache.commons.lang.StringUtils;

import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.util.BytesUtil;

/**
 * VaporizingRow is a transient row. It lives in the specified heap. It is
 * vaporized when the heap is gone. This class is mostly used in updating data
 * in Humpback. It is friendly with EA.
 * 
 * @author wgu0
 */
public final class VaporizingRow {
    Heap heap;
    int maxColumnId;
    long pValueArray;
    long pKey;
    long version;
    int size = 0;

    public VaporizingRow(Heap heap, int maxColumnId) {
        this.heap = heap;
        this.maxColumnId = maxColumnId;
        init();
    }

    VaporizingRow(Heap heap, SlowRow from) {
        if (from.getMaxColumnId() < 0) {
            throw new IllegalArgumentException();
        }
        this.heap = heap;
        this.maxColumnId = from.getMaxColumnId();
        init();
        this.version = from.getTrxTimestamp();
        byte[] key = from.getKey();
        long pKey = (key != null) ? KeyBytes.allocSet(heap, key).getAddress() : 0;
        setKey(pKey);
        for (int i = 0; i <= this.maxColumnId; i++) {
            Object value = from.get(i);
            set(i, value);
        }
    }

    private void init() {
        int len = (maxColumnId + 1) * 8;
        this.pValueArray = this.heap.alloc(len);
        Unsafe.setMemory(this.pValueArray, len, (byte) 0);
    }

    public int getMaxColumnId() {
        return maxColumnId;
    }

    public void setKey(byte[] key) {
        long pKey = (key != null) ? KeyBytes.allocSet(heap, key).getAddress() : 0;
        setKey(pKey);
    }

    public void setKey(long pNewKey) {
        if (this.pKey != 0) {
            size -= FishObject.getSize(this.pKey);
        }
        this.pKey = pNewKey;
        size += FishObject.getSize(pKey);
    }

    public void set(int field, Object value) {
        long pValue = FishObject.allocSet(heap, value);
        setFieldAddress(field, pValue);
    }

    public long getFieldAddress(int index) {
        long ppValue = this.pValueArray + index * 8;
        long pValue = Unsafe.getLong(ppValue);
        return pValue;
    }

    public void setFieldAddress(int n, long pValue) {
        if ((n < 0) || (n > this.maxColumnId)) {
            throw new IllegalArgumentException();
        }
        long ppValue = this.pValueArray + n * 8;
        long pOldValue = Unsafe.getLong(ppValue);
        if (pOldValue != 0) {
            size -= FishObject.getSize(pOldValue);
        }
        size += FishObject.getSize(pValue);
        Unsafe.putLong(ppValue, pValue);
    }

    public long getKeyAddress() {
        return this.pKey;
    }

    public Object get(int index) {
        long pValue = getFieldAddress(index);
        Object value = FishObject.get(heap, pValue);
        return value;
    }

    /**
     * number of bytes
     * 
     * @return
     */
    public int getSize() {
        return this.size + Row.getHeaderSize(this.maxColumnId);
    }

    public long getTrxTimestamp() {
        return this.version;
    }

    public long getVersion() {
        return this.version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("version:");
        buf.append(this.version);
        buf.append('\n');
        buf.append("max column id:");
        buf.append(getMaxColumnId());
        buf.append('\n');
        buf.append("trx timestamp:");
        buf.append(getTrxTimestamp());
        buf.append('\n');
        buf.append("key:");
        buf.append(BytesUtil.toHex8(getKey()));
        for (int i = 0; i <= getMaxColumnId(); i++) {
            Object value = get(i);
            if (value != null) {
                buf.append('\n');
                buf.append(i);
                buf.append(":");
                String output;
                if (value instanceof byte[]) {
                    output = BytesUtil.toHex((byte[]) value);
                }
                else {
                    output = value.toString();
                }
                if (output.length() >= 80) {
                    output = StringUtils.left(output, 80) + "...";
                }
                buf.append(output);
            }
        }
        return buf.toString();
    }

    public byte[] getKey() {
        long pKey = getKeyAddress();
        byte[] key = (byte[]) FishObject.get(heap, pKey);
        return key;
    }

    public static VaporizingRow from(Heap heap, int maxColumnId, Row raw) {
        VaporizingRow row = new VaporizingRow(heap, maxColumnId);
        row.setKey(raw.getKeyAddress());
        row.setVersion(raw.getTrxTimestamp());
        for (int i = 0; i <= raw.getMaxColumnId(); i++) {
            row.setFieldAddress(i, raw.getFieldAddress(i));
        }
        return row;
    }

}
