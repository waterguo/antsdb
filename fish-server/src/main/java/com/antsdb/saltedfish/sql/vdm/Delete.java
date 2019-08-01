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

import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.sql.LockLevel;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.meta.TableMeta;

public class Delete extends DeleteBase {
    CursorMaker maker;
    int tableId;
    
    public Delete(Orca orca, TableMeta table, GTable gtable, CursorMaker maker) {
        super(orca, table, gtable);
        this.maker = maker;
        this.tableId = table.getId();
    }

    @Override
    public Object run(VdmContext ctx, Parameters params) {
        ctx.getSession().lockTable(this.tableId, LockLevel.SHARED, true);
        try (Cursor c = this.maker.make(ctx, params, 0)) {
            int count = 0;
            for (;;) {
                long pRecord = c.next();
                if (pRecord == 0) {
                    break;
                }
                long pKey = Record.get(pRecord, 0);
                if (deleteSingleRow(ctx, params, pKey)) {
                    count++;
                }
            }
            return count;
        }
    }

    @Override
    List<TableMeta> getDependents() {
        List<TableMeta> list = new ArrayList<TableMeta>();
        list.add(this.table);
        return list;
    }
    
    @Override
    public void explain(int level, List<ExplainRecord> records) {
        ExplainRecord rec = new ExplainRecord(-1, level, "delete");
        records.add(rec);
        this.maker.explain(level, records);
    }
}
