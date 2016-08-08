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
 * @author wgu0
 */
public class SysMetaRow {
    final static int COLUMN_TABLE_ID = 2;
    final static int COLUMN_NAMESPACE = 3;
    final static int COLUMN_COMPARATOR = 4;
    final static int COLUMN_TABLE_NAME = 5;
    
	SlowRow row;

	SysMetaRow(int id) {
		this.row = new SlowRow(id);
	}
	
	SysMetaRow(SlowRow row) {
		super();
		this.row = row;
	}
	
	public String getNamespace() {
		return (String)this.row.get(COLUMN_NAMESPACE);
	}
	
	void setNamespace(String value) {
		this.row.set(COLUMN_NAMESPACE, value);
	}
	
	public int getTableId() {
		return (int)this.row.get(COLUMN_TABLE_ID);
	}

	void setTableId(int value) {
		this.row.set(COLUMN_TABLE_ID, value);
	}
	
	void setTableName(String value) {
		this.row.set(COLUMN_TABLE_NAME, value);
	}

	public String getTableName() {
		return (String)this.row.get(COLUMN_TABLE_NAME);
	}
}
