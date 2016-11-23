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
package com.antsdb.saltedfish.sql.meta;

import com.antsdb.saltedfish.nosql.SlowRow;

public class RuleColumnMeta {
	
	private SlowRow row;

	public RuleColumnMeta(int id) {
		this.row = new SlowRow(id);
		setId(id);
	}
	
	public RuleColumnMeta(SlowRow row) {
		this.row = row;
	}
	
	private void setId(int value) {
        row.set(ColumnId.sysrulecol_id.getId(), value);
	}
	
	public int getId() {
        return (Integer)row.get(ColumnId.sysrulecol_id.getId());
	}
	
	public byte[] getKey() {
		return this.row.getKey();
	}
	
	public void setRuleId(int value) {
        row.set(ColumnId.sysrulecol_rule_id.getId(), value);
	}
	
	public int getRuleId() {
        return (Integer)row.get(ColumnId.sysrulecol_rule_id.getId());
	}
	
	public void setColumnId(int value) {
        row.set(ColumnId.sysrulecol_column_id.getId(), value);
	}
	
	public int getColumnId() {
		return (Integer)row.get(ColumnId.sysrulecol_column_id.getId());
	}

	public void setTrxTimestamp(long value) {
		this.row.setTrxTimestamp(value);
	}

	public SlowRow getRow() {
		return this.row;
	}
}
