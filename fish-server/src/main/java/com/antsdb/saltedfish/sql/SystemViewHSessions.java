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
import java.util.List;

import com.antsdb.saltedfish.nosql.HumpbackSession;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.sql.vdm.View;
import com.antsdb.saltedfish.util.CursorUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class SystemViewHSessions extends View {
    public static class Line {
        public Integer ID;
        public String ENDPOINT;
        public Long TRXTS;
    }
    
    public SystemViewHSessions() {
        super(CursorUtil.toMeta(Line.class));
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        List<Line> result = new ArrayList<>();
        for (HumpbackSession i:ctx.getHumpback().getSessions()) {
            Line line = new Line();
            line.ID = i.getId();
            line.ENDPOINT = i.getEndpoint();
            line.TRXTS = i.getOpenTime();
            result.add(line);
        }
        return CursorUtil.toCursor(this.meta, result);
    }

}
