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
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.util.CodingError;
import com.antsdb.saltedfish.util.TypeSafeUtil;

public class HumpbackRecord extends Record {
    Row row;
    int[] mapping;
    CursorMeta meta;
    
    public HumpbackRecord(CursorMeta meta, int[] mapping, Row row) {
        super();
        this.mapping = mapping;
        this.row = row;
        this.meta = meta;
    }

    @Override
    public Object get(int field) {
        int idx = this.mapping[field];
        Object val = row.get(idx);
        if (val == null) {
            return val;
        }
        DataType type = meta.getColumn(field).getType();
        if (type.getJavaType() == val.getClass()) {
            return val;
        }
        else {
            val = TypeSafeUtil.upcast(type.getJavaType(), val);
            return val;
        }
    }

    @Override
    public Record set(int field, Object val) {
    	throw new CodingError();
    }

    @Override
    public byte[] getKey() {
        return this.row.getKey();
    }

    @Override
    public int size() {
        return this.mapping.length;
    }

    public long getFieldAddress(int field) {
        int idx = this.mapping[field];
        long addr = row.getFieldAddress(idx);
        return addr;
    }

	public long getKeyAddress() {
		return this.row.getKeyAddress();
	}
}
