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

import java.util.List;

import com.antsdb.saltedfish.cpp.FishBoundary;
import com.antsdb.saltedfish.cpp.FlexibleHeap;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.sql.LockLevel;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;

public class UpdateSingleRow extends UpdateBase {
    Operator keyExpr;
    int[] mapping;
    CursorMeta meta = new CursorMeta();
    int tableId;

    public UpdateSingleRow(
            Orca orca, 
            TableMeta table, 
            GTable gtable, 
            Operator key, 
            List<ColumnMeta> columns, 
            List<Operator> exprs) {
            super(orca, table, gtable, columns, exprs);
            this.tableId = table.getId();
        this.keyExpr = key;
        this.mapping = new int[table.getColumns().size()];
        for (int i=0; i<table.getColumns().size(); i++) {
            ColumnMeta col = table.getColumns().get(i);
            this.meta.addColumn(FieldMeta.valueOf(table, col));
            this.mapping[i] = col.getColumnId();
        }
    }

    @Override
    public Object run(VdmContext ctx, Parameters params) {
        ctx.getSession().lockTable(this.tableId, LockLevel.SHARED, true);
        try (Heap heap = new FlexibleHeap()) {
            long pBoundary = this.keyExpr.eval(ctx, heap, params, 0);
            if (pBoundary == 0) {
                return 0;
            }
            FishBoundary boundary = new FishBoundary(pBoundary);
            long pKey = boundary.getKeyAddress();
            return updateSingleRow(ctx, heap, params, pKey) ? 1 : 0;
        }
    }
    
    @Override
    public void explain(int level, List<ExplainRecord> records) {
        ExplainRecord rec = new ExplainRecord(-1, level, "Table Seek (" + this.table.getObjectName() + ")");
        records.add(rec);
    }
}