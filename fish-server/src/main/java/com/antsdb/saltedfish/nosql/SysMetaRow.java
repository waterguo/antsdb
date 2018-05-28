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

/**
 * 
 * @author wgu0
 */
public class SysMetaRow {
    final static int COLUMN_TABLE_ID = 1;
    final static int COLUMN_NAMESPACE = 2;
    final static int COLUMN_COMPARATOR = 3;
    final static int COLUMN_TABLE_NAME = 4;
    final static int COLUMN_TABLE_TYPE = 5;
    final static int COLUMN_DELETE_MARK = 6;
    
    SlowRow row;

    public SysMetaRow(int id) {
        this.row = new SlowRow(id);
        setTableId(id);
    }
    
    public SysMetaRow(SlowRow row) {
        super();
        this.row = row;
    }
    
    public String getNamespace() {
        return (String)this.row.get(COLUMN_NAMESPACE);
    }
    
    public void setNamespace(String value) {
        this.row.set(COLUMN_NAMESPACE, value);
    }
    
    public int getTableId() {
        return (int)this.row.get(COLUMN_TABLE_ID);
    }

    public void setTableId(int value) {
        this.row.set(COLUMN_TABLE_ID, value);
    }
    
    public void setTableName(String value) {
        this.row.set(COLUMN_TABLE_NAME, value);
    }

    public String getTableName() {
        return (String)this.row.get(COLUMN_TABLE_NAME);
    }

    public void setType(TableType type) {
        this.row.set(COLUMN_TABLE_TYPE, type.toString());
    }
    
    public TableType getType() {
        return TableType.valueOf((String)this.row.get(COLUMN_TABLE_TYPE));
    }
    
    public void setDeleted(boolean value) {
        this.row.set(COLUMN_DELETE_MARK, value);
    }
    
    public boolean isDeleted() {
        Boolean value = (Boolean)this.row.get(COLUMN_DELETE_MARK);
        return (value == null) ? false : value;
    }
}
