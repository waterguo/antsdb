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

import java.sql.Timestamp;
import java.util.ArrayList;

import com.antsdb.saltedfish.sql.Orca.DeadSession;
import com.antsdb.saltedfish.sql.vdm.Cursor;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.sql.vdm.ViewMaker;
import com.antsdb.saltedfish.util.BytesUtil;
import com.antsdb.saltedfish.util.CursorUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class SlowQueries extends ViewMaker {

    public static class Line {
        public Integer SESSION_ID;
        public Long DURATION;
        public Timestamp TIMESTAMP;
        public String QUERY;
        public String PARAMS;
    }

    public SlowQueries() {
        super(CursorUtil.toMeta(Line.class));
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        ArrayList<Line> result = new ArrayList<>();
        for (Session i:ctx.getOrca().getSessions()) {
            for (Execution j:i.slowOnes.values()) {
                result.add(addLine(i, j));
            }
        }
        for (DeadSession i:ctx.getOrca().deadSessions) {
            for (Execution j:i.session.slowOnes.values()) {
                result.add(addLine(i.session, j));
            }
        }
        Cursor c = CursorUtil.toCursor(meta, result);
        return c;
    }

    private Line addLine(Session session, Execution exec) {
        Line result = new Line();
        result.SESSION_ID = session.getId();
        result.TIMESTAMP = new Timestamp(exec.timestamp);
        result.DURATION = exec.duration;
        result.QUERY = exec.sql.toString();
        result.PARAMS = getParams(exec);
        return result;
    }

    private String getParams(Execution exec) {
        if (exec.params == null) {
            return "";
        }
        StringBuilder buf = new StringBuilder();
        for (int i=0; i<exec.params.size(); i++) {
            Object value = exec.params.get(i);
            String string;
            if (value == null) {
                string = "null";
            }
            else if (value instanceof byte[]) {
                string = BytesUtil.toCompactHex((byte[])value);
            }
            else {
                string = value.toString();
            }
            if (string.length() > 50) {
                string = string.substring(0, 49) + "...";
            }
            buf.append(string);
            buf.append(",");
        }
        if (buf.length() > 0) {
            buf.deleteCharAt(buf.length()-1);
        }
        return buf.toString();
    }
}
