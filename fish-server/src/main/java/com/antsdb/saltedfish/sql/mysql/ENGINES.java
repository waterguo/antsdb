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

import java.util.Collections;

import com.antsdb.saltedfish.sql.vdm.CursorMeta;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.sql.vdm.View;
import com.antsdb.saltedfish.util.CursorUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class ENGINES extends View {
    private static final CursorMeta META = CursorUtil.toMeta(Line.class);

    public static class Line {
        public String ENGINE;
        public String SUPPORT;
        public String COMMENT;
        public String TRANSACTIONS;
        public String XA;
        public String SAVEPOINTS;
    }
    
    public ENGINES() {
        super(META);
    }
    
    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        Line line = new Line();
        line.ENGINE = "InnoDB";
        line.SUPPORT = "DEFAULT";
        line.COMMENT = "";
        line.TRANSACTIONS = "YES";
        line.XA = "YES";
        line.SAVEPOINTS = "YES";
        return CursorUtil.toCursor(META, Collections.singletonList(line));
    }
}
