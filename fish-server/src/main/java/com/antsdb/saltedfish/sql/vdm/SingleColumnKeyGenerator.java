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

import org.apache.commons.lang.NotImplementedException;

import com.antsdb.saltedfish.nosql.SlowRow;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.util.UberUtil;

public class SingleColumnKeyGenerator extends PrimaryKeyGenerator {
    ColumnMeta col;
    
    public SingleColumnKeyGenerator(ColumnMeta col) {
        super();
        this.col = col;
    }

    @Override
    byte[] generate(Orca orca, SlowRow row) {
        // keep in mind, row value can be anything, not necessarily consistent with column data type
        
        Object value = row.get(col.getColumnId());
        byte[] key = generate(value);
        return key;
    }

    public byte[] generate(Object value) {
        byte[] key;
        if (value == null) {
            key = null;
        }
        else if (col.getDataType().getJavaType() == Integer.class) {
            Integer val = (Integer)UberUtil.toObject(Integer.class, value);
            key = KeyMaker.make(val);
        }
        else if (col.getDataType().getJavaType() == Long.class) {
            Long val = (Long)UberUtil.toObject(Long.class, value);
            key = KeyMaker.make(val);
        }
        else if (col.getDataType().getJavaType() == String.class) {
            String val = (String)UberUtil.toObject(String.class, value);
            key = UberUtil.toUtf8(val);
        }
        else {
            throw new NotImplementedException();
        }
        return key;
    }
}
