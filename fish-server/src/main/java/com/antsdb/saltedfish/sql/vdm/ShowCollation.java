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

import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.planner.SortKey;
import com.antsdb.saltedfish.util.CursorUtil;

public class ShowCollation extends CursorMaker {
    CursorMeta meta = new CursorMeta();

    public ShowCollation() {
        meta.addColumn(new FieldMeta("Collation", DataType.varchar()))
            .addColumn(new FieldMeta("Charset", DataType.varchar()))
            .addColumn(new FieldMeta("Id", DataType.longtype()))
            .addColumn(new FieldMeta("Default", DataType.varchar()))
            .addColumn(new FieldMeta("Compiled", DataType.varchar()))
            .addColumn(new FieldMeta("Sortlen", DataType.longtype()));
    }

    @Override
    public CursorMeta getCursorMeta() {
        return this.meta;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        // wgu0: hard coded for now
        Record rec = new HashMapRecord();
        rec.set(0, "latin1_bin")
           .set(1, "latin1")
           .set(2, 47l)
           .set(3, "")
           .set(4, "Yes")
           .set(5, 0);
        Cursor c = CursorUtil.toCursor(meta, rec);
        return c;
    }

    @Override
    public boolean setSortingOrder(List<SortKey> order) {
        return false;
    }

}
