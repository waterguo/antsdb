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
package com.antsdb.saltedfish.sql.vdm;

import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.sql.meta.ColumnId;

/**
 * 
 * @author wgu0
 */
public class SysRuleRow {
	Row row;

    public SysRuleRow(Row row) {
        this.row = row;
    }

    public Integer getId() {
    	return (Integer)row.get(ColumnId.sysrule_id.getId());
    }
    
    public Integer getTableId() {
    	return (Integer)row.get(ColumnId.sysrule_table_id.getId());
    }

    public String getRuleName() {
    	return (String)row.get(ColumnId.sysrule_rule_name.getId());
    }

    public Integer getRuleType() {
    	return (Integer)row.get(ColumnId.sysrule_rule_type.getId());
    }

    public Boolean isUnique() {
    	Integer value = (Integer)row.get(ColumnId.sysrule_is_unique.getId());
    	return (value != null) ? value == 1 : false;
    }

	@Override
	public String toString() {
		String string = String.format("%s [id=%d type=%s unique=%b tableId=%d] ",
				getRuleName(),
				getId(),
				getRuleType(),
				isUnique(),
				getIndexTableId());
		return string;
	}

	public Integer getIndexTableId() {
    	return (Integer)row.get(ColumnId.sysrule_index_table_id.getId());
	}
	
    public int[] getRuleColumns() {
        int[] columns = (int[])this.row.get(ColumnId.sysrule_columns.getId());
        return columns;
    }
}
