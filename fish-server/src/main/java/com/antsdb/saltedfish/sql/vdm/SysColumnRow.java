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

import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.sql.meta.ColumnId;

/**
 * 
 * @author wgu0
 */
public class SysColumnRow {
    Row row;

    public SysColumnRow(Row row) {
        this.row = row;
    }

    public String getNamespace() {
        return (String)row.get(ColumnId.syscolumn_namespace.getId());
    }
    
    public String getTableName() {
        return (String)row.get(ColumnId.syscolumn_table_name.getId());
    }

    public String getColumnName() {
        return (String)row.get(ColumnId.syscolumn_column_name.getId());
    }

    public String getTypeName() {
        return (String)row.get(ColumnId.syscolumn_type_name.getId());
    }

    public Integer getLength() {
        return (Integer)row.get(ColumnId.syscolumn_type_length.getId());
    }
    
    public Integer getScale() {
        return (Integer)row.get(ColumnId.syscolumn_type_scale.getId());
    }
    
    public Integer getColumnId() {
        return (Integer)row.get(ColumnId.syscolumn_column_id.getId());
    }

    public Boolean isNullable() {
        return (Boolean)row.get(ColumnId.syscolumn_nullable.getId());
    }

    public String getDefaultValue() {
        return (String)row.get(ColumnId.syscolumn_default_value.getId());
    }

    public int getTableId() {
        return (int)row.get(ColumnId.syscolumn_table_id.getId());
    }
    
    @Override
    public String toString() {
        String string = String.format("%s [columnId=%d type=%s length=%s scale=%d nullable=%b default=%s]",
                getColumnName(),
                getColumnId(),
                getTypeName(),
                getLength(),
                getScale(),
                isNullable(),
                getDefaultValue());
        return string;
    }

    public Integer getId() {
        return (Integer)row.get(ColumnId.syscolumn_id.getId());
    }

}
