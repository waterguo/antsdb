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

import java.util.ArrayList;
import java.util.List;

import com.antsdb.saltedfish.util.CursorUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class ShowMasterStatus extends ViewMaker {
    public static class Line {
        public String File = "";
        public Long Position;
        public String Binlog_Do_DB = "";
        public String Binlog_Ignore_DB = "";
    }
    
    public ShowMasterStatus() {
        super(CursorUtil.toMeta(Line.class));
    }
    
    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        List<Line> result = new ArrayList<>();
        Line line = new Line();
        line.Position = ctx.getHumpback().getSpaceManager().getAllocationPointer();
        result.add(line);
        Cursor c = CursorUtil.toCursor(getCursorMeta(), result);
        return c;
    }
}
