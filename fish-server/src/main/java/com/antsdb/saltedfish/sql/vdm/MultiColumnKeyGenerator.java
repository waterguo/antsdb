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

import java.util.List;

import com.antsdb.saltedfish.nosql.SlowRow;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;

public class MultiColumnKeyGenerator extends PrimaryKeyGenerator {
    List<ColumnMeta> columns;
    int keysize;
    
    public MultiColumnKeyGenerator(List<ColumnMeta> columns) {
        super();
        this.columns = columns;
        this.keysize = KeyUtil.getSize(columns);
    }

    @Override
    byte[] generate(Orca orca, SlowRow row) {
        byte[] bytes = new byte[this.keysize];
        int pos = 0;
        for (ColumnMeta col:columns) {
            Object value = row.get(col.getColumnId());
            pos += KeyUtil.write(col, bytes, pos, value);
        }
        return bytes;
    }

    public byte[] generate(List<Object> values) {
        
        // TODO Auto-generated method stub
        return null;
    }
}
