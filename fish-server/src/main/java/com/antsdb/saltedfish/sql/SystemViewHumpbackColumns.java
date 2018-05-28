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

import java.util.ArrayList;

import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.HColumnRow;
import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.RowIterator;
import com.antsdb.saltedfish.nosql.SlowRow;
import com.antsdb.saltedfish.sql.vdm.Cursor;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.sql.vdm.ViewMaker;
import com.antsdb.saltedfish.util.CursorUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class SystemViewHumpbackColumns extends ViewMaker {
    private Orca orca;

    public static class Line {
        public Integer TABLE_ID;
        public Integer COLUMN__POS;
        public String COLUMN_NAME;
        public Integer DELETE_MARK;
    }
    
    SystemViewHumpbackColumns(Orca orca) {
        super(CursorUtil.toMeta(Line.class));
        this.orca = orca;
    }
    
    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        ArrayList<Line> list = new ArrayList<>();
        GTable sysmeta = this.orca.getHumpback().getTable(Humpback.SYSCOLUMN_TABLE_ID);
        for (RowIterator i=sysmeta.scan(0, Long.MAX_VALUE, true);i.next();) {
            list.add(toLine(i.getRow()));
        }
        Cursor c = CursorUtil.toCursor(meta, list);
        return c;
    }

    private Line toLine(Row row) {
        HColumnRow meta = new HColumnRow(SlowRow.from(row));
        Line result = new Line();
        result.TABLE_ID = meta.getTableId();
        result.COLUMN__POS = meta.getColumnPos();
        result.COLUMN_NAME = meta.getColumnName();
        result.DELETE_MARK = meta.isDeleted() ? 1 : 0;
        return result;
    }
}
