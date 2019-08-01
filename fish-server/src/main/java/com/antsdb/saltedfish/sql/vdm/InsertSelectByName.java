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

import java.util.HashMap;
import java.util.Map;

import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;

/**
 * 
 * @author *-xguo0<@
 */
public class InsertSelectByName extends Statement {
    private ObjectName tableName;
    private CursorMaker maker;

    public InsertSelectByName(ObjectName tableName, CursorMaker maker) {
        this.tableName = tableName;
        this.maker = maker;
    }
    
    @Override
    public Object run(VdmContext ctx, Parameters params) {
        TableMeta table = Checks.tableExist(ctx.getSession(), this.tableName);
        Operator[] values = new Operator[table.getMaxColumnId()+1];
        Map<String, FieldValue> inputs = new HashMap<>();
        for (int i=0; i<this.maker.getCursorMeta().getColumns().size(); i++) {
            FieldMeta fm = this.maker.getCursorMeta().getColumn(i);
            inputs.put(fm.getName().toLowerCase(), new FieldValue(fm, i));
        }
        for (int i=0; i<table.getColumns().size(); i++) {
            ColumnMeta ii = table.getColumns().get(i);
            FieldValue match = inputs.get(ii.getColumnName().toLowerCase());
            values[ii.getColumnId()] = match!=null ? match : new NullValue(); 
        }
        GTable gtable = ctx.getHumpback().getTable(table.getHtableId());
        InsertSelect inserts = new InsertSelect(ctx.getOrca(), gtable, table, this.maker, values);
        return inserts.run(ctx, params);
    }
}
