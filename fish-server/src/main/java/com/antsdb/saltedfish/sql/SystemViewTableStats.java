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

import com.antsdb.saltedfish.nosql.TableStats;
import com.antsdb.saltedfish.sql.vdm.Cursor;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.sql.vdm.ViewMaker;
import com.antsdb.saltedfish.util.CursorUtil;
import com.antsdb.saltedfish.util.UberFormatter;

/**
 * 
 * @author *-xguo0<@
 */
public class SystemViewTableStats extends ViewMaker {
    private Orca orca;

    public static class Line {
        public Integer TABLE_ID;
        public Long ROW_COUNT;
        public Integer MIN_ROW_SIZE;
        public Integer MAX_ROW_SIZE;
        public Integer AVERAGE_ROW_SIZE;
        public String HASH;
    }
    
    SystemViewTableStats(Orca orca) {
        super(CursorUtil.toMeta(Line.class));
        this.orca = orca;
    }
    
    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        ArrayList<Line> list = new ArrayList<>();
        for (TableStats i:this.orca.getHumpback().getStatistician().getStats().values()) {
            list.add(addLine(i));
        }
        Cursor c = CursorUtil.toCursor(meta, list);
        return c;
    }

    private Line addLine(TableStats table) {
        Line result = new Line();
        result.TABLE_ID = table.tableId;
        result.ROW_COUNT = table.count;
        result.MIN_ROW_SIZE = table.minRowSize;
        result.MAX_ROW_SIZE = table.maxRowSize;
        result.AVERAGE_ROW_SIZE = (int)table.averageRowSize;
        result.HASH = UberFormatter.hex(table.hash);
        return result;
    }
}
