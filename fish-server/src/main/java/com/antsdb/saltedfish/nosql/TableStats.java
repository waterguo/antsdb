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

import java.util.concurrent.ConcurrentHashMap;

/**
 * 
 * @author *-xguo0<@
 */
public class TableStats {
    static final int COLUMN_TABLE_ID = 1;
    static final int COLUMN_SP = 2;
    static final int COLUMN_COUNT = 3;
    static final int COLUMN_CHECKSUM = 4;
    static final int COLUMN_MIN_ROW_SIZE = 5;
    static final int COLUMN_MAX_ROW_SIZE = 6;
    static final int COLUMN_AVERAGE_ROW_SIZE = 7;
    
    public int tableId;
    public long sp;
    public long count;
    public long hash;
    public int minRowSize = Integer.MAX_VALUE;
    public int maxRowSize;
    public double averageRowSize;
    public ConcurrentHashMap<Integer, ColumnStats> columnStats = new ConcurrentHashMap<>();
    public boolean isDirty = false;
    
    SlowRow save() {
        SlowRow row = new SlowRow(this.tableId);
        row.set(COLUMN_TABLE_ID, this.tableId);
        row.set(COLUMN_SP, this.sp);
        row.set(COLUMN_COUNT, this.count);
        row.set(COLUMN_CHECKSUM, this.hash);
        row.set(COLUMN_MIN_ROW_SIZE, this.minRowSize);
        row.set(COLUMN_MAX_ROW_SIZE, this.maxRowSize);
        row.set(COLUMN_AVERAGE_ROW_SIZE, this.averageRowSize);
        this.isDirty = false;
        return row;
    }
    
    public TableStats load(Row row) {
        this.tableId = (Integer)row.get(COLUMN_TABLE_ID);
        this.sp = (Long)row.get(COLUMN_SP);
        this.count = (Long)row.get(COLUMN_COUNT);
        this.hash = (Long)row.get(COLUMN_CHECKSUM);
        this.minRowSize = (Integer)row.get(COLUMN_MIN_ROW_SIZE);
        this.maxRowSize = (Integer)row.get(COLUMN_MAX_ROW_SIZE);
        this.averageRowSize = (Double)row.get(COLUMN_AVERAGE_ROW_SIZE);
        return this;
    }
    
    private void setMaxRowSize(int value) {
        value = Math.max(this.maxRowSize, value);
        this.maxRowSize = value;
    }

    private void setMinRowSize(int value) {
        value = Math.min(this.minRowSize, value);
        this.minRowSize = value;
    }

    private void setAverageRowSize(double value) {
        this.averageRowSize = (int)value;
    }

    private void setCount(long value) {
        this.count = value;
    }
    
    void inspectInsert(Row row, long sp) {
        int rowSize = row.getLength();
        setMinRowSize(rowSize);
        setMaxRowSize(rowSize);
        setAverageRowSize((averageRowSize * count + rowSize) / (count + 1));
        if (count != -1) {
            count++;
        }
        this.sp = sp;
        this.isDirty = true;
    }
    
    void inspectUpdate(Row row, long sp) {
        int rowSize = row.getLength();
        setMinRowSize(rowSize);
        setMaxRowSize(rowSize);
        setAverageRowSize((averageRowSize * (count - 1) + rowSize) / count);
        this.sp = sp;
        this.isDirty = true;
    }
    
    void inspectPut(Row row, long sp) {
        int rowSize = row.getLength();
        setMinRowSize(rowSize);
        setMaxRowSize(rowSize);
        setCount(-1);
        setAverageRowSize(-1);
        this.sp = sp;
        this.isDirty = true;
    }
    
    void inspectDelete(long sp) {
        if (count != -1) {
            count--;
        }
        this.sp = sp;
        this.isDirty = true;
    }
}
