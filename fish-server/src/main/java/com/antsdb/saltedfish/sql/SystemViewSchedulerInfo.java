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

import com.antsdb.saltedfish.nosql.Knob;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.sql.vdm.View;
import com.antsdb.saltedfish.util.CursorUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class SystemViewSchedulerInfo extends View {
    public class Line {
        public String name;
        public Boolean active;
        public int priority;
        public Boolean yield;
        public String state;
        public String progress;
    }

    public SystemViewSchedulerInfo() {
        super(CursorUtil.toMeta(Line.class));
    }
    
    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        ArrayList<Line> result = new ArrayList<>();
        for (Knob i:ctx.getOrca().getScheduler().getKnobs()) {
            Line line = new Line();
            line.active = !i.isPaused();
            line.name = i.getName();
            line.state = i.getThread() != null ? i.getThread().getState().toString() : null;
            line.priority = i.getPriority();
            line.yield = i.isSleeping();
            line.progress = i.getProgress();
            result.add(line);
        }
        return CursorUtil.toCursor(this.meta, result);
    }
}
