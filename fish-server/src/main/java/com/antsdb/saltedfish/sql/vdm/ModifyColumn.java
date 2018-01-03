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

import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.MetadataService;
import com.antsdb.saltedfish.sql.meta.TableMeta;

public class ModifyColumn extends Statement implements ColumnAttributes {
    ObjectName tableName;
    String oldName;
    String columnName;
    DataType type;
    boolean nullable;
    String defaultValue;
	boolean autoIncrement = false;
    public String enumValues;
    
	public ModifyColumn(ObjectName tableName, String oldName) {
		this.tableName = tableName;
		this.oldName = oldName;
	}
	
    @Override
    public Object run(VdmContext ctx, Parameters params) {
        // create metadata

    	    MetadataService meta = ctx.getOrca().getMetaService();
        Transaction trx = ctx.getTransaction();
        TableMeta table = Checks.tableExist(ctx.getSession(), this.tableName);
        ColumnMeta column = Checks.columnExist(table, oldName);
        column = (ColumnMeta)column.clone();
        column.setColumnName(this.columnName);
        column.setType(this.type);
        column.setNullable(this.nullable);
        if (this.defaultValue != null) {
        	    column.setDefault(this.defaultValue);
        }
        column.setAutoIncrement(this.autoIncrement);
        meta.modifyColumn(trx, column);
        
        // done
        
        return null;
    }

	@Override
	public String getColumnName() {
		return this.columnName;
	}

	@Override
	public ColumnAttributes setColumnName(String name) {
		this.columnName = name;
		return this;
	}

	@Override
	public DataType getType() {
		return this.type;
	}

	@Override
	public ColumnAttributes setType(DataType type) {
		this.type = type;
		return this;
	}

	@Override
	public boolean isNullable() {
		return this.nullable;
	}

	@Override
	public ColumnAttributes setNullable(boolean b) {
		this.nullable = b;
		return this;
	}

	@Override
	public String getDefaultValue() {
		return this.defaultValue;
	}

	@Override
	public ColumnAttributes setDefaultValue(String value) {
		this.defaultValue = value;
		return this;
	}

	@Override
	public boolean isAutoIncrement() {
		return this.isAutoIncrement();
	}

	@Override
	public ColumnAttributes setAutoIncrement(boolean value) {
		this.autoIncrement = value;
		return this;
	}

	@Override
	public String getEnumValues() {
		return enumValues;
	}

	@Override
	public void setEnumValues(String enumValues) {
		this.enumValues = enumValues;
	}
}
