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
package com.antsdb.saltedfish.sql.meta;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.antsdb.saltedfish.nosql.SlowRow;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.util.CodingError;
import com.antsdb.saltedfish.util.UberObject;

public abstract class RuleMeta<T> extends UberObject {
    public static enum Rule {
        PrimaryKey,
        Index,
        ForeignKey,
    }
    
    SlowRow row;

    public RuleMeta(Orca orca, Rule type, int tableId) {
        int id = (int)orca.getIdentityService().getNextGlobalId();
        row = new SlowRow(tableId, id);
        setId(id);
        setType(type);
    }
    
    protected RuleMeta(SlowRow row) {
        super();
        this.row = row;
    }

    @Override
    public String toString() {
        return getName();
    }
    
    public void setType(Rule type) {
        row.set(ColumnId.sysrule_rule_type.getId(), type.ordinal());
    }

    int getId() {
        return (Integer)this.row.get(ColumnId.sysrule_id.getId());
    }
    
    void setId(int id) {
        this.row.set(ColumnId.sysrule_id.getId(), id);
    }
    
    void setTableId(int id) {
        row.set(ColumnId.sysrule_table_id.getId(), id);
    }
    
    public int getTableId() {
        return (int)row.get(ColumnId.sysrule_table_id.getId());
    }
    
    @SuppressWarnings("unchecked")
    public T setName(String name) {
        row.set(ColumnId.sysrule_rule_name.getId(), name);
        return (T)this;
    }
    
    public String getName() {
        return (String)row.get(ColumnId.sysrule_rule_name.getId());
    }
    
    public void setRuleColumns(List<ColumnMeta> columns) {
        int[] array = new int[columns.size()];
        for (int i=0; i<columns.size(); i++) {
            array[i] = columns.get(i).getId();
        }
        setRuleColumns(array);
    }
    
    public void setRuleColumns(int[] value) {
        this.row.set(ColumnId.sysrule_columns.getId(), value);
    }
    
    public int[] getRuleColumns() {
        int[] columns = (int[])this.row.get(ColumnId.sysrule_columns.getId());
        return columns;
    }
    
    public List<ColumnMeta> getColumns(TableMeta table) {
        List<ColumnMeta> list = new ArrayList<ColumnMeta>();
        for (int columnId:getRuleColumns()) {
            ColumnMeta col = table.getColumn(columnId);
            if (col == null) {
                throw new CodingError();
            }
            list.add(col);
        }
        return list;
    }

    public List<String> getColumnNames(TableMeta table) {
        List<String> result = new ArrayList<>();
        for (ColumnMeta i:table.getColumns()) {
            result.add(i.getColumnName());
        }
        return result;
    }
    
    public String getNamespace() {
        return (String)this.row.get(ColumnId.sysrule_namespace.getId());
    }
    
    public void setNamespace(String value) {
        this.row.set(ColumnId.sysrule_namespace.getId(), value);
    }
    
    public boolean conform(TableMeta table, List<String> columns) {
        List<ColumnMeta> keyColumns = getColumns(table);
        if (keyColumns.size() != columns.size()) {
            return false;
        }
        for (int i=0; i<columns.size(); i++) {
            if (!keyColumns.get(i).getColumnName().equalsIgnoreCase(columns.get(i))) {
                return false;
            }
        }
        return true;
    }
    
    public boolean conform(TableMeta table, String[] columns) {
        return conform(table, Arrays.asList(columns));
    }
}
