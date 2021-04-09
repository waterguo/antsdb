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
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.antsdb.saltedfish.sql.planner.Node;
import com.antsdb.saltedfish.sql.planner.Planner;
import com.antsdb.saltedfish.sql.planner.RowSet;
import com.antsdb.saltedfish.util.CursorUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class Analyze extends Instruction {
    CursorMeta meta = CursorUtil.toMeta(Line.class);
    List<Line> lines = new ArrayList<>();
    
    public class Line {
        public String NAME;
        public Integer VERSION;
        public String CONDITIONS;
    }
    
    public Analyze(Planner planner) {
        Operator where = planner.getWhere();
        Line line = new Line();
        line.NAME = "WHERE";
        line.CONDITIONS = where!=null ? where.toString() : null;
        this.lines.add(line);
        for (Map.Entry<ObjectName, Node> i:planner.getNodes().entrySet()) {
            for (RowSet j:i.getValue().getUnion()) {
                line = new Line();
                line.NAME = i.getKey().toString();
                line.VERSION = j.tag;
                line.CONDITIONS = StringUtils.join(j.conditions, ',');
                this.lines.add(line);
            }
        }
    }
    
    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        return CursorUtil.toCursor(this.meta, lines);
    }
}
