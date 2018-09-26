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

import java.sql.Timestamp;
import java.util.ArrayList;

import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.minke.Minke;
import com.antsdb.saltedfish.minke.MinkeCache;
import com.antsdb.saltedfish.minke.MinkeFile;
import com.antsdb.saltedfish.minke.MinkePage;
import com.antsdb.saltedfish.minke.PageState;
import com.antsdb.saltedfish.nosql.StorageEngine;
import com.antsdb.saltedfish.sql.vdm.Cursor;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.sql.vdm.View;
import com.antsdb.saltedfish.util.CursorUtil;
import static com.antsdb.saltedfish.util.UberFormatter.*;

/**
 * 
 * @author *-xguo0<@
 */
public class SystemViewPages extends View {
    Orca orca;
    
    public static class Line {
        public String PAGE_ID;
        public int TABLE_ID;
        public String OFFSET;
        public String STATE;
        public long HITS;
        public Timestamp LAST_READ;
        public String START_KEY;
        public String END_KEY;
    }
    
    public SystemViewPages(Orca orca) {
        super(CursorUtil.toMeta(Line.class));
        this.orca = orca;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        ArrayList<Line> list = new ArrayList<>();
        Minke minke = getMinke(); 
        if (orca.getHumpback().getStorageEngine() instanceof Minke)
        for (MinkeFile file:minke.getFiles()) {
            if (file == null) {
                continue;
            }
            for (MinkePage page:file.getPages()) {
                list.add(toLine(page));
            }
        }
        
        // done
        Cursor c = CursorUtil.toCursor(meta, list);
        return c;
    }

    private Line toLine(MinkePage page) {
        Line result = new Line();
        result.TABLE_ID = page.getTableId();
        result.PAGE_ID = hex(page.getId());
        result.HITS = page.getHits();
        result.STATE = PageState.getStateName(page.getState());
        result.START_KEY = KeyBytes.toString(page.getStartKeyPointer());
        result.END_KEY = KeyBytes.toString(page.getEndKeyPointer());
        long lastRead = page.getLastRead();
        result.LAST_READ = (lastRead == 0) ? null : new Timestamp(lastRead);
        return result;
    }

    private Minke getMinke() {
        Minke result = null;
        StorageEngine stor = this.orca.getHumpback().getStorageEngine();
        if (stor instanceof Minke) {
            result = (Minke)stor;
        }
        else if (stor instanceof MinkeCache) {
            result = ((MinkeCache)stor).getMinke();
        }
        return result;
    }
}
