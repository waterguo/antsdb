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
package com.antsdb.saltedfish.sql;

import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.sql.meta.TableId;
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
public class SysTable extends CursorMaker {
    CursorMeta meta = new CursorMeta();
    Orca orca;
    int[] mapping;
    
    SysTable(Orca orca) {
        this.orca = orca;
        meta.addColumn(new FieldMeta("id", DataType.integer()));
        meta.addColumn(new FieldMeta("namespace", DataType.varchar()));
        meta.addColumn(new FieldMeta("table_name", DataType.varchar()));
        meta.addColumn(new FieldMeta("table_type", DataType.varchar()));
        meta.addColumn(new FieldMeta("ext_name", DataType.varchar()));
        this.mapping = new int[] {1, 2, 3, 4, 5, 6};
    }
    
    @Override
    public CursorMeta getCursorMeta() {
        return this.meta;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        GTable table = ctx.getHumpback().getTable(TableId.SYSTABLE);
        Cursor cursor = new DumbCursor(
                ctx.getSpaceManager(),
                this.meta, 
                table.scan(ctx.getTransaction().getTrxId(), ctx.getTransaction().getTrxTs()), 
                mapping,
                ctx.getCursorStats(0));
        return cursor;
    }

}
