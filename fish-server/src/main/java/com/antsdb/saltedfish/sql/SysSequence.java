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
import com.antsdb.saltedfish.sql.vdm.CursorMeta;
import com.antsdb.saltedfish.sql.vdm.DumbCursor;
import com.antsdb.saltedfish.sql.vdm.FieldMeta;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.sql.vdm.View;

/**
 * 
 * @author *-xguo0<@
 */
public class SysSequence extends View {
    static CursorMeta META = new CursorMeta();
    
    Orca orca;
    int[] mapping;
    
    static {
        META.addColumn(new FieldMeta("id", DataType.integer()));
        META.addColumn(new FieldMeta("namespace", DataType.varchar()));
        META.addColumn(new FieldMeta("sequence_name", DataType.varchar()));
        META.addColumn(new FieldMeta("last_number", DataType.longtype()));
        META.addColumn(new FieldMeta("seed", DataType.longtype()));
        META.addColumn(new FieldMeta("increment", DataType.longtype()));
    }
    
    SysSequence(Orca orca) {
        super(META);
        this.orca = orca;
        this.mapping = new int[] {1, 2, 3, 4, 5, 6};
    }
    
    @Override
    public CursorMeta getCursorMeta() {
        return this.meta;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        GTable table = ctx.getHumpback().getTable(TableId.SYSSEQUENCE);
        Cursor cursor = new DumbCursor(
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
