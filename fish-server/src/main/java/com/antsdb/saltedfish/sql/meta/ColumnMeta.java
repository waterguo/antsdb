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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;

import com.antsdb.saltedfish.nosql.SlowRow;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.DataTypeFactory;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.vdm.ObjectName;
import com.antsdb.saltedfish.util.BytesUtil;
import com.antsdb.saltedfish.util.UberUtil;

public class ColumnMeta extends MetaObject {
	static Logger _log = UberUtil.getThisLogger();
	
    SlowRow row;
    DataType type;
    
    private ColumnMeta() {
    }
    
    public ColumnMeta(Orca orca, long trxts, TableMeta table, String columnName) {
        int columnId;
        if (Orca.SYSNS.equals(table.getNamespace())) {
            columnId = ColumnId.valueOf(table.getTableName(), columnName).getId();
        }
        else {
            columnId = table.getMaxColumnId() + 1;
        }
        int id = (int)orca.getIdentityService().getNextGlobalId(); 
        this.row = new SlowRow(table.getId(), id);
        setId(id);
        setColumnId(columnId);
        setNamespace(table.getNamespace());
        setTableName(table.getTableName());
        setColumnName(columnName);
        setTableId(table.getId());
        if (getTypeName() != null) {
        	this.type = orca.getTypeFactory().newDataType(getTypeName(), getTypeLength(), getTypeScale());
        }
    }

    public ColumnMeta(DataTypeFactory fac, SlowRow row) {
        super();
        this.row = row;
        if (getTypeName() != null) {
            this.type = fac.newDataType(getTypeName(), getTypeLength(), getTypeScale());
        }
    }
    
    public int getId() {
        return (Integer)row.get(ColumnId.syscolumn_id.getId());
    }
    
    public ColumnMeta setId(int id) {
        row.set(ColumnId.syscolumn_id.getId(), id);
        return this;
    }
    
    public String getNamespace() {
        return (String)row.get(ColumnId.syscolumn_namespace.getId());
    }
    
    void setNamespace(String name) {
        row.set(ColumnId.syscolumn_namespace.getId(), name);
    }
    
    public String getTableName() {
        return (String)row.get(ColumnId.syscolumn_table_name.getId());
    }
    
    void setTableName(String name) {
        row.set(ColumnId.syscolumn_table_name.getId(), name);
    }
    
    public String getColumnName() {
        return (String)row.get(ColumnId.syscolumn_column_name.getId());
    }

    public ColumnMeta setColumnName(String name) {
        row.set(ColumnId.syscolumn_column_name.getId(), name);
        return this;
    }

    public String getTypeName() {
        return (String)row.get(ColumnId.syscolumn_type_name.getId());
    }
    
    public ColumnMeta setType(DataType type) {
        setTypeName(type.getName());
        setTypeLength(type.getLength());
        setTypeScale(type.getScale());
        this.type = type;
        return this;
    }
    
    private void setTypeName(String string) {
        row.set(ColumnId.syscolumn_type_name.getId(), string);
    }

    public int getTypeLength() {
        return (int)row.get(ColumnId.syscolumn_type_length.getId());
    }
    
    private void setTypeLength(int length) {
        row.set(ColumnId.syscolumn_type_length.getId(), length);
    }

    public int getTypeScale() {
        return (int)row.get(ColumnId.syscolumn_type_scale.getId());
    }
    
    private void setTypeScale(int scale) {
        row.set(ColumnId.syscolumn_type_scale.getId(), scale);
    }

    public boolean isNullable() {
        return (boolean)row.get(ColumnId.syscolumn_nullable.getId());
    }
    
    public void setNullable(boolean nullable) {
        row.set(ColumnId.syscolumn_nullable.getId(), nullable);
    }

    public String getDefault() {
        return (String)row.get(ColumnId.syscolumn_default_value.getId());
    }
    
    public ColumnMeta setDefault(String value) {
        row.set(ColumnId.syscolumn_default_value.getId(), value);
        return this;
    }
    
    public int getOrdinalPosition() {
        return 1;
    }
    
    public byte[] getKey() {
        return this.row.getKey();
    }
    
    static byte[] getKey(ObjectName tableName, String columnName) {
        return BytesUtil.toUtf8(tableName.getNamespace() + "." + tableName.getTableName() + "." + columnName);
    }

    public DataType getDataType() {
        return type;
    }

    public long getTimeId() {
        return (long)row.get(ColumnId.syscolumn_time_id.getId());
    }
    
    public void setTimeId(long timeId) {
        row.set(ColumnId.syscolumn_time_id.getId(), timeId);
    }

    public boolean isAutoIncrement() {
    	Boolean result = (Boolean)row.get(ColumnId.syscolumn_auto_increment.getId());
    	return (result != null) ? result : false;
    }
    
	public void setAutoIncrement(boolean autoIncrement) {
        row.set(ColumnId.syscolumn_auto_increment.getId(), autoIncrement);
	}
	
	public int getColumnId() {
    	Integer result = (Integer)row.get(ColumnId.syscolumn_column_id.getId());
    	return result;
	}
	
	public void setColumnId(int columnId) {
		row.set(ColumnId.syscolumn_column_id.getId(), columnId);
	}
	
	public String getCollation() {
		return (String)row.get(ColumnId.syscolumn_collation.getId());
	}

	public void setCollation(String value) {
		row.set(ColumnId.syscolumn_collation.getId(), value);
	}
	
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append(getTableName());
        buf.append('.');
        buf.append(getColumnName());
        buf.append('{');
        for (Map.Entry<Integer, Object> i:this.row.entrySet()) {
            if (i.getValue() == null) {
                continue;
            }
            String fieldName = ColumnId.getFieldName(i.getKey());
            fieldName = (fieldName == null) ? "" : fieldName;
            buf.append('"');
            buf.append(fieldName);
            buf.append('"');
            buf.append(':');
            if (i.getValue() instanceof String) {
                buf.append('"');
                buf.append(i.getValue());
                buf.append('"');
            }
            else {
                buf.append(i.getValue().toString());
            }
            buf.append(';');
        }
        buf.deleteCharAt(buf.length()-1);
        buf.append('}');
        return buf.toString();
    }

	@Override
	public ColumnMeta clone() {
		ColumnMeta newone =  new ColumnMeta();
		newone.row = this.row.clone();
		newone.type = this.type;
		return newone;
	}

	public Map<String, Integer> getEnumValueMap() {
		String values = getEnumValues();
		if (values == null) {
			return null;
		}
		Map<String, Integer> map = new HashMap<>();
		int j=1;
		for (String i:StringUtils.split(values, ',')) {
			i = i.substring(1, i.length()-1);
			map.put(i, j);
			j++;
		}
		return map;
	}
	
	public String getEnumValues() {
		return (String)row.get(ColumnId.syscolumn_enum_values.getId());
	}

	public void setEnumValues(String value) {
		row.set(ColumnId.syscolumn_enum_values.getId(), value);
	}
	
	public float getSequence() {
		Float value = (Float)row.get(ColumnId.syscolumn_seq.getId());
		return (value == null) ? getId() : value;
	}
	
	public void setSequence(float value) {
		row.set(ColumnId.syscolumn_seq.getId(), value);
	}

    public void setTableId(int value) {
        row.set(ColumnId.syscolumn_table_id.getId(), value);
    }

    public int getTableId() {
        return (Integer)row.get(ColumnId.syscolumn_table_id.getId());
    }
}
