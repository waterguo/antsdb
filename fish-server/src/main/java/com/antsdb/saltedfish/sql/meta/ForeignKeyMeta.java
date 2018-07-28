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

import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.antsdb.saltedfish.nosql.SlowRow;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.vdm.ObjectName;

/**
 * 
 * @author *-xguo0<@
 */
public class ForeignKeyMeta extends RuleMeta<ForeignKeyMeta> {

    public ForeignKeyMeta(Orca orca, TableMeta table) {
        super(orca, Rule.ForeignKey, table.getId());
        setTableId(table.getId());
    }

    public ForeignKeyMeta(SlowRow row) {
        super(row);
    }

    public void setParentTable(ObjectName value) {
        row.set(ColumnId.sysrule_parent_table_name.getId(), value.toString());
    }
    
    public ObjectName getParentTable() {
        String result = (String)row.get(ColumnId.sysrule_parent_table_name.getId());
        return ObjectName.parse(result);
    }
    
    public void setParentColumns(List<String> value) {
        row.set(ColumnId.sysrule_parent_column_names.getId(), StringUtils.join(value, ','));
    }
    
    public String[] getParentColumns() {
        String value = (String)row.get(ColumnId.sysrule_parent_column_names.getId());
        if (value == null) {
            return null;
        }
        String[] result = StringUtils.split(value, ',');
        return result;
    }
    
    @Override
    public ForeignKeyMeta clone() {
        ForeignKeyMeta result = new ForeignKeyMeta(this.row.clone());
        return result;
    }

    public void setOnDelete(String value) {
        row.set(ColumnId.sysrule_on_delete.getId(), value);
    }

    public String getOnDelete() {
        return (String)row.get(ColumnId.sysrule_on_delete.getId());
    }

    public void setOnUpdate(String value) {
        row.set(ColumnId.sysrule_on_update.getId(), value);
    }

    public String getOnUpdate() {
        return (String)row.get(ColumnId.sysrule_on_update.getId());
    }

}
