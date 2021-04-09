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

import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.planner.SortKey;
import com.antsdb.saltedfish.util.CursorUtil;

public class ShowEngines extends CursorMaker {
    CursorMeta meta = new CursorMeta();

    public ShowEngines() {
        meta.addColumn(new FieldMeta("Engine", DataType.varchar()))
            .addColumn(new FieldMeta("Support", DataType.varchar()))
            .addColumn(new FieldMeta("Comment", DataType.varchar()))
            .addColumn(new FieldMeta("Transactions", DataType.varchar()))
            .addColumn(new FieldMeta("XA", DataType.varchar()))
            .addColumn(new FieldMeta("Savepoints", DataType.varchar()));
    }

    @Override
    public CursorMeta getCursorMeta() {
        return this.meta;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        // wgu0: hard coded for now
        Record rec = new HashMapRecord();
        rec.set(0, "InnoDB")
           .set(1, "DEFAULT")
           .set(2, "Percona-XtraDB, Supports transactions, row-level locking, and foreign keys")
           .set(3, "YES")
           .set(4, "NO")
           .set(5, "NO");
        Cursor c = CursorUtil.toCursor(meta, rec);
        return c;
    }

    @Override
    public boolean setSortingOrder(List<SortKey> order) {
        return false;
    }
    
    @Override
    public float getScore() {
        return 0;
    }
}
