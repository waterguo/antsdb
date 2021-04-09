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

import com.antsdb.saltedfish.sql.planner.SortKey;
import com.antsdb.saltedfish.util.CursorUtil;

/**
 * making the current master record as a cursor
 * 
 * @author xguo
 *
 */
public class MasterRecordCursorMaker extends CursorMaker {
    CursorMeta meta;
    
    public MasterRecordCursorMaker(CursorMeta meta, int makerId) {
        this.meta = meta;
        setMakerId(makerId);
    }

    @Override
    public CursorMeta getCursorMeta() {
        return meta;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        ctx.getCursorStats(makerId).incrementAndGet();
        Cursor c = CursorUtil.toCursor(meta, pMaster);
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
