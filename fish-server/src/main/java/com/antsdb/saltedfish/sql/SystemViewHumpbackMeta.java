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

import java.util.ArrayList;

import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.RowIterator;
import com.antsdb.saltedfish.nosql.SlowRow;
import com.antsdb.saltedfish.nosql.SysMetaRow;
import com.antsdb.saltedfish.sql.vdm.Cursor;
import com.antsdb.saltedfish.sql.vdm.CursorMaker;
import com.antsdb.saltedfish.sql.vdm.CursorMeta;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.util.CursorUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class SystemViewHumpbackMeta extends CursorMaker {
    private Orca orca;
    private CursorMeta meta;

    public static class Line {
        public Integer TABLE_ID;
        public String NAMESPACE;
        public String TABLE_NAME;
        public String TABLE_TYPE;
        public Integer DELETE_MARK;
    }
    
    SystemViewHumpbackMeta(Orca orca) {
        this.orca = orca;
        this.meta = CursorUtil.toMeta(Line.class);
    }
    
    @Override
    public CursorMeta getCursorMeta() {
        return this.meta;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        ArrayList<Line> list = new ArrayList<>();
        GTable sysmeta = this.orca.getHumpback().getTable(Humpback.SYSMETA_TABLE_ID);
        for (RowIterator i=sysmeta.scan(0, Long.MAX_VALUE);i.next();) {
            list.add(toLine(i.getRow()));
        }
        Cursor c = CursorUtil.toCursor(meta, list);
        return c;
    }

    private Line toLine(Row row) {
        SysMetaRow meta = new SysMetaRow(SlowRow.from(row));
        Line result = new Line();
        result.TABLE_ID = meta.getTableId();
        result.NAMESPACE = meta.getNamespace();
        result.TABLE_NAME = meta.getTableName();
        result.TABLE_TYPE = meta.getType().toString();
        result.DELETE_MARK = meta.isDeleted() ? 1 : 0;
        return result;
    }
}
