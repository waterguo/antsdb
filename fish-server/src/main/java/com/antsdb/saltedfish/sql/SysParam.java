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
package com.antsdb.saltedfish.sql;

import java.util.List;

import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.sql.meta.TableId;
import com.antsdb.saltedfish.sql.planner.SortKey;
import com.antsdb.saltedfish.sql.vdm.Cursor;
import com.antsdb.saltedfish.sql.vdm.CursorMaker;
import com.antsdb.saltedfish.sql.vdm.CursorMeta;
import com.antsdb.saltedfish.sql.vdm.DumbCursor;
import com.antsdb.saltedfish.sql.vdm.FieldMeta;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.VdmContext;

/**
 * 
 * @author *-xguo0<@
 */
public class SysParam extends CursorMaker {
    CursorMeta meta = new CursorMeta();
    Orca orca;
    int[] mapping;
    
    SysParam(Orca orca) {
        this.orca = orca;
        meta.addColumn(new FieldMeta("name", DataType.varchar()));
        meta.addColumn(new FieldMeta("type", DataType.varchar()));
        meta.addColumn(new FieldMeta("value", DataType.varchar()));
        this.mapping = new int[] {1, 2, 3};
    }
    
    @Override
    public CursorMeta getCursorMeta() {
        return this.meta;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        GTable table = ctx.getHumpback().getTable(TableId.SYSPARAM);
        Cursor cursor = new DumbCursor(
                ctx.getSpaceManager(),
                this.meta, 
                table.scan(ctx.getTransaction().getTrxId(), ctx.getTransaction().getTrxTs(), true), 
                mapping,
                ctx.getCursorStats(0));
        return cursor;
    }

    @Override
    public boolean setSortingOrder(List<SortKey> order) {
        return false;
    }

}
