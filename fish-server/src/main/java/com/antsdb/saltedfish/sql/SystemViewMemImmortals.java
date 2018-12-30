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
import java.util.concurrent.atomic.AtomicLong;

import com.antsdb.saltedfish.cpp.MemoryManager;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.sql.vdm.View;
import com.antsdb.saltedfish.util.CursorUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class SystemViewMemImmortals extends View {
    public static class Line {
        public Integer POINT;
        public Long SIZE;
    }
    
    SystemViewMemImmortals() {
        super(CursorUtil.toMeta(Line.class));
    }
    
    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        AtomicLong[] immortals = MemoryManager.getImmortals();
        ArrayList<Line> result = new ArrayList<>();
        for (int i=0; i<immortals.length; i++) {
            Line line = new Line();
            line.POINT = i;
            line.SIZE = immortals[i].get();
            result.add(line);
        }
        return CursorUtil.toCursor(meta, result);
    }
}
