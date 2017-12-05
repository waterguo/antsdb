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
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.RowIterator;
import com.antsdb.saltedfish.nosql.SlowRow;
import com.antsdb.saltedfish.sql.meta.TableId;
import com.antsdb.saltedfish.sql.meta.UserMeta;
import com.antsdb.saltedfish.sql.vdm.Cursor;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.sql.vdm.ViewMaker;
import com.antsdb.saltedfish.util.CursorUtil;

/**
 * 
 * @author *-xguo0<@
 */
class SysUser extends ViewMaker {
    public static class Line {
        public Integer ID;
        public String NAME;
    }

    SysUser() {
        super(CursorUtil.toMeta(Line.class));
    }
    
    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        ArrayList<Line> list = new ArrayList<>();
        GTable sysuser = ctx.getHumpback().getTable(TableId.SYSUSER);
        for (RowIterator i=sysuser.scan(0, Long.MAX_VALUE, true);;) {
            if (!i.next()) {
                break;
            }
            Row row = i.getRow();
            UserMeta user = new UserMeta(SlowRow.from(row));
            if (user.isDeleted()) {
                continue;
            }
            list.add(toLine(user));
        }
        Cursor c = CursorUtil.toCursor(meta, list);
        return c;
    }

    private Line toLine(UserMeta user) {
        Line result = new Line();
        result.ID = user.getId();
        result.NAME = user.getName();
        return result;
    }

}
