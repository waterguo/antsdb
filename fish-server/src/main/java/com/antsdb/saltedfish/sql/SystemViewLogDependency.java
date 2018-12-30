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

import com.antsdb.saltedfish.nosql.LogDependency;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.sql.vdm.View;
import com.antsdb.saltedfish.util.CursorUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class SystemViewLogDependency extends View {

    public static class Line {
        public String NAME;
        public String LOG_POINTER_HEX;
        public Long LOG_POINTER;
    }
    
    public SystemViewLogDependency() {
        super(CursorUtil.toMeta(Line.class));
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        List<Line> result = new ArrayList<>();
        add("", result, ctx.getHumpback().getLogDependency());
        return CursorUtil.toCursor(this.meta, result);
    }

    private void add(String parent, List<Line> result, LogDependency dependency) {
        if (dependency == null) {
            return;
        }
        Line line = new Line();
        line.NAME = parent.equals("/") ? parent + dependency.getName() : parent + "/" + dependency.getName();
        line.LOG_POINTER = dependency.getLogPointer();
        line.LOG_POINTER_HEX = Long.toHexString(line.LOG_POINTER);
        result.add(line);
        if (dependency.getChildren() != null) {
            for (LogDependency i:dependency.getChildren()) {
                add(line.NAME, result, i);
            }
        }
    }
}
