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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import com.antsdb.saltedfish.nosql.SlowRow;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;

public class CursorMeta {
	private static ColumnMeta KEY = new ColumnMeta(null, new SlowRow(0));
	private static ColumnMeta ROWID = new ColumnMeta(null, new SlowRow(0));
	
	static {
		KEY.setColumnId(-1);
		ROWID.setColumnId(0);
	}
	
    List<FieldMeta> fields = new ArrayList<FieldMeta>();
	ObjectName source;
    public CursorMeta parent;
    
    public CursorMeta() {
    }
    
    public CursorMeta(CursorMeta parent) {
        this.parent = parent;
    }
    
    public int getColumnCount() {
        int count = this.fields.size();
        if (this.parent != null) {
            count += this.parent.getColumnCount();
        }
        return count;
    }

    public List<FieldMeta> getColumns() {
    	if (this.parent == null) {
    		return this.fields;
    	}
        ArrayList<FieldMeta> list = new ArrayList<>();
        addFields(list, this);
        return list;
    }

    private void addFields(ArrayList<FieldMeta> list, CursorMeta meta) {
    	if (meta.parent != null) {
    		addFields(list, meta.parent);
    	}
        list.addAll(meta.fields);
	}

	public FieldMeta getColumn(int pos) {
        int start = 0;
        if (this.parent != null) {
            start = this.parent.getColumnCount();
        }
        if (pos < start) {
            return this.parent.getColumn(pos);
        }
        else {
            return this.fields.get(pos - start);
        }
    }

    public int getColumnPosition(String columnLabel) {
        int pos = -1;
        if (this.parent != null) {
            pos = this.parent.getColumnPosition(columnLabel);
        }
        if (pos == -1) {
            for (int i=0; i<this.fields.size(); i++) {
                if (columnLabel.equals(this.fields.get(i).getName())) {
                    pos = i;
                    if (this.parent != null) {
                        pos += this.parent.getColumnCount();
                    }
                }
            }
        }
        return pos;
    }
    
    public CursorMeta addColumn(FieldMeta column) {
        this.fields.add(column);
        return this;
    }
    
    public static CursorMeta join(CursorMeta left, CursorMeta right) {
        CursorMeta meta = new CursorMeta();
        left.getColumns().forEach(it-> {
            meta.fields.add(it);
        });
        for (FieldMeta i:right.getColumns()) {
            meta.fields.add(i);
        }
        return meta;
    }
    
    public static CursorMeta from(ObjectName name, TableMeta table) {
        CursorMeta cursorMeta = new CursorMeta();
        cursorMeta.source = name;
        for (ColumnMeta i:table.getColumns()) {
            cursorMeta.fields.add(FieldMeta.valueOf(i));
        }
        return cursorMeta;
    }

    public int findColumn(ColumnMeta column) {
    	for (int i=0; i<this.fields.size(); i++) {
    		FieldMeta ii = this.fields.get(i);
    		if (ii.getColumn() == column) {
    			return i;
    		}
    	}
    	return -1;
    }
    
    public int findColumn(Predicate<FieldMeta> predicate) {
        int pos = -1;
        for (int i=0; i<this.fields.size(); i++) {
            FieldMeta column = this.fields.get(i); 
            if (predicate.test(column)) {
                if (pos >= 0) {
                    throw new OrcaException("Column is ambiguous: " + column);
                }
                pos = i;
                if (this.parent != null) {
                    pos += this.parent.getColumnCount();
                }
            }
        }
        if (pos < 0) {
        	if (this.parent != null) {
        		pos = this.parent.findColumn(predicate);
        	}
        }
        return pos;
    }

    public List<FieldMeta> getFields() {
		return getColumns();
	}

	public void setFields(List<FieldMeta> fields) {
		this.fields = fields;
	}

	public int[] getHumpbackMapping() {
        int[] mapping = new int[fields.size()];
        for (int i=0; i<mapping.length; i++) {
            FieldMeta field = this.fields.get(i);
            ColumnMeta column = field.getColumn();
            mapping[i] = column.getColumnId();
        }
        return mapping;
	}
	
	public static CursorMeta from(TableMeta table) {
        CursorMeta cursorMeta = new CursorMeta();
        cursorMeta.source = table.getObjectName();
        FieldMeta fieldKey = new FieldMeta();
        fieldKey.setName("*key");
        fieldKey.setType(DataType.blob());
        fieldKey.setTableAlias(table.getTableName());
        fieldKey.setColumn(KEY);
        cursorMeta.fields.add(fieldKey);
        FieldMeta fieldRowid = new FieldMeta();
        fieldRowid.setName("*rowid");
        fieldRowid.setType(DataType.longtype());
        fieldRowid.setTableAlias(table.getTableName());
        fieldRowid.setColumn(ROWID);
        cursorMeta.fields.add(fieldRowid);
        for (ColumnMeta column:table.getColumns()) {
            cursorMeta.fields.add(FieldMeta.valueOf(column));
        }
        return cursorMeta;
	}
}
