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
package com.antsdb.saltedfish.sql.mysql;

import java.util.ArrayList;
import java.util.List;

import com.antsdb.saltedfish.sql.Session;
import com.antsdb.saltedfish.sql.vdm.CursorMeta;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.sql.vdm.View;
import com.antsdb.saltedfish.util.CursorUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class ShowProcesslist extends View {
    private static final CursorMeta META = CursorUtil.toMeta(Line.class);
    
    public static class Line {
        public Long Id;
        public String User;
        public String Host;
        public String db;
        public String Command;
        public Integer Time;
        public String State;
        public String Info;
        public Double Progress;
    }

    public ShowProcesslist() {
        super(META);
    }

    @Override
    public Object run(VdmContext ctx, Parameters notused, long pMaster) {
        List<Line> result = new ArrayList<>();
        for (Session i:ctx.getOrca().getSessions()) {
            if (i == ctx.getOrca().getDefaultSession()) {
                continue;
            }
            result.add(toLine(i));
        }
        return CursorUtil.toCursor(META, result);
    }

    private Line toLine(Session session) {
        Line result = new Line();
        result.Id = (long)session.getId();
        result.User = session.getUser();
        result.Host = session.remote;
        result.db = session.getCurrentNamespace();
        result.Info = session.getSql();
        return result;
    }
}
