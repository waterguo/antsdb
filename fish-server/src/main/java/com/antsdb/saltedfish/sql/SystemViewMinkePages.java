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
import java.util.Collections;

import com.antsdb.saltedfish.minke.Minke;
import com.antsdb.saltedfish.minke.MinkeCache;
import com.antsdb.saltedfish.minke.MinkeFile;
import com.antsdb.saltedfish.minke.MinkePage;
import com.antsdb.saltedfish.nosql.StorageEngine;
import com.antsdb.saltedfish.sql.vdm.Cursor;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.sql.vdm.View;
import com.antsdb.saltedfish.util.CursorUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class SystemViewMinkePages extends View {
    public class Line {
        public Integer PAGE_ID;
        public Integer TABLE_ID;
        public Long LAST_ACCESS;
        public Integer STATE;
        public String CONVERAGE;
    }
    
    SystemViewMinkePages(int a) {
        super(CursorUtil.toMeta(Line.class));
    }
    
    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        Minke minke;
        StorageEngine stor = ctx.getHumpback().getStorageEngine();
        if (stor instanceof Minke) {
            minke = (Minke)stor;
        }
        else if (stor instanceof MinkeCache) {
            minke = ((MinkeCache)stor).getMinke();
        }
        else {
            return CursorUtil.toCursor(meta, Collections.emptyList());
        }
        ArrayList<Line> list = new ArrayList<>();
        for (MinkeFile i:minke.getFiles()) {
            if (i != null) {
                for (MinkePage j:i.getPages()) {
                    list.add(addLine(j));
                }
            }
        }
        Cursor c = CursorUtil.toCursor(meta, list);
        return c;
    }

    private Line addLine(MinkePage page) {
        Line result = new Line();
        result.PAGE_ID = page.getId();
        result.TABLE_ID = page.getTableId();
        result.STATE = page.getState();
        result.LAST_ACCESS = page.getLastRead();
        result.CONVERAGE = page.getCoverage();
        return result;
    }

}
