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

/**
 * 
 * @author *-xguo0<@
 */
public class HColumnRow {
    final static int COLUMN_TABLE_ID = 1;
    final static int COLUMN__POS = 2;
    final static int COLUMN_NAME = 3;
    final static int DELETE_MARK = 4;

    SlowRow row;

    public HColumnRow(int tableId, int columnPos) {
        this.row = new SlowRow(tableId, columnPos);
        setTableId(tableId);
        setColumnPos(columnPos);
        setDeleted(false);
    }
    
    public HColumnRow(SlowRow row) {
        this.row = row;
    }
    
    public int getColumnPos() {
        return (int)this.row.get(COLUMN__POS);
    }
    
    private void setColumnPos(int columnPos) {
        this.row.set(COLUMN__POS, columnPos);
    }

    public int getTableId() {
        return (int)this.row.get(COLUMN_TABLE_ID);
    }
    
    private void setTableId(int tableId) {
        this.row.set(COLUMN_TABLE_ID, tableId);
    }

    public String getColumnName() {
        return (String)this.row.get(COLUMN_NAME);
    }
    
    public void setColumnName(String value) {
        this.row.set(COLUMN_NAME, value);
    }
    
    public boolean isDeleted() {
        return (boolean)this.row.get(DELETE_MARK);
    }
    
    public void setDeleted(boolean value) {
        this.row.set(DELETE_MARK, value);
    }
}
