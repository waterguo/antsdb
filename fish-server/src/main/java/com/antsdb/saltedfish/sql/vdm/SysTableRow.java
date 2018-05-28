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

import org.apache.commons.lang.StringUtils;

import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.sql.meta.ColumnId;

public class SysTableRow {
    Row row;

    public SysTableRow(Row row) {
        super();
        this.row = row;
    }
    
    public String getNamespace() {
        return (String)this.row.get(2);
    }
    
    public String getTableName() {
        return (String)this.row.get(3);
    }
    
    public int getId() {
    	return (Integer)this.row.get(1);
    }

    public String dump() {
        StringBuilder buf = new StringBuilder();
        buf.append(toString());
        buf.append('\n');
        for (int i=0; i<=row.getMaxColumnId(); i++) {
            Object value = row.get(i);
            if (value == null) {
                continue;
            }
            String column = ColumnId.valueOf("systable", i).toString();
            column = StringUtils.removeStart(column, "systable_");
            String text = String.format("  %s:%s", column, value.toString());
            buf.append(text);
            buf.append('\n');
        }
        buf.deleteCharAt(buf.length()-1);
        return buf.toString();
    }
    
	@Override
	public String toString() {
		return String.format("%s.%s [id=%d]", getNamespace(), getTableName(), getId());
	}
}
