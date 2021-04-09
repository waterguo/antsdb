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

import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;

public class FieldMeta {
    private int matchWeight = 0;
    private TableMeta table;
    private ColumnMeta column;
    
    private String tableAlias;
    private String name;
    private ObjectName sourceTable;
    private String sourceColumn;
    private DataType type;
    private boolean isKeyColumn = false;
    
    public FieldMeta() {}
    
    public ObjectName getSourceTableName() {
        return sourceTable;
    }

    public FieldMeta setSourceColumnName(String name) {
        this.sourceColumn = name;
        return this;
    }
    
    public FieldMeta setSourceTable(ObjectName name) {
        this.sourceTable = name;
        return this;
    }
    
    public FieldMeta(String name, DataType type) {
        this.name = name;
        this.type = type;
    }
    
    public static FieldMeta valueOf(ColumnMeta column) {
        FieldMeta cursorColumn = new FieldMeta();
        cursorColumn.name = column.getColumnName();
        cursorColumn.type = column.getDataType();
        cursorColumn.tableAlias = column.getTableName();
        cursorColumn.column = column;
        return cursorColumn;
    }

    public static FieldMeta valueOf(TableMeta table, ColumnMeta column) {
        FieldMeta cursorColumn = valueOf(column);
        cursorColumn.table = table;
        cursorColumn.setSourceTable(table.getObjectName());
        return cursorColumn;
    }
    
    public String getName() {
        return name;
    }

    public FieldMeta setName(String name) {
        this.name = name;
        return this;
    }

    public DataType getType() {
        return type;
    }

    public void setType(DataType type) {
        this.type = type;
    }
    
    public String getTableAlias() {
        return this.tableAlias;
    }
    
    /**
     * source table.
     * 
     * @return nullable
     */
    public ObjectName getSourceTable() {
        return this.sourceTable;
    }
    
    /**
     * source column
     * 
     * @return nullable
     */
    public String getSourceName() {
        return this.sourceColumn;
    }

    @Override
    public Object clone() {
        FieldMeta newone = new FieldMeta();
        newone.name = this.name;
        newone.matchWeight = this.matchWeight;
        newone.sourceColumn = this.sourceColumn;
        newone.sourceTable = this.sourceTable;
        newone.tableAlias = this.tableAlias;
        newone.type = this.type;
        return newone;
    }

    @Override
    public String toString() {
        return getTableAlias() + "." + getName();
    }

    public int getMatchWeight() {
        return this.matchWeight;
    }

    public TableMeta getTable() {
        return this.table;
    }

    public ObjectName getTableName() {
        return this.sourceTable;
    }

    public ColumnMeta getColumn() {
        return this.column;
    }

    public void setColumn(ColumnMeta col) {
        this.column = col;
    }
    
    public Object setMatchWeight(int matchWeight2) {
        return this.matchWeight = matchWeight2;
    }

    public void setTableAlias(String alias) {
        this.tableAlias = alias;
    }

    public void setKeyColumn(boolean value) {
        this.isKeyColumn = value;
    }
    
    public boolean isKeyColumn() {
        return this.isKeyColumn;
    }
}
