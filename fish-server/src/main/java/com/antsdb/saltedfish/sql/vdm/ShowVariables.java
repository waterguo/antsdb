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
import java.util.TreeMap;

import com.antsdb.saltedfish.sql.planner.SortKey;
import com.antsdb.saltedfish.util.CursorUtil;
import com.antsdb.saltedfish.util.MysqlColumnMeta;

public class ShowVariables extends CursorMaker {
    static CursorMeta META = CursorUtil.toMeta(Line.class, "information_schema", "VARIABLES");
    private boolean isGlobal;

    public static class Line {
        @MysqlColumnMeta(column="VARIABLE_NAME")
        public String Variable_name;
        @MysqlColumnMeta(column="VARIABLE_VALUE")
        public String Value;
    }

    public ShowVariables(boolean global) {
        this.isGlobal = global;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        Map<String, String> map = new TreeMap<>();
        map.putAll(ctx.getOrca().getConfig().getAll());
        if (!this.isGlobal) {
            map.putAll(ctx.getSession().getConfig().getAll());
        }
        List<Line> list = new ArrayList<>();
        map.entrySet().forEach(it -> {
            Line line = new Line();
            line.Variable_name = it.getKey();
            line.Value = it.getValue();
            list.add(line);
        });
        Cursor c = CursorUtil.toCursor(META, list);
        return c;
        /*
        Map<String, Object> variables = new HashMap<String, Object>();
        List<Record> list = new ArrayList<>();
        variables.entrySet().forEach((it) -> {
            Record rec = new HashMapRecord();
            rec.set(0, it.getKey())
               .set(1, it.getValue().toString());
            list.add(rec);
        });
        */
    }

    @Override
    public CursorMeta getCursorMeta() {
        return META;
    }

    @Override
    public boolean setSortingOrder(List<SortKey> order) {
        return false;
    }

    @Override
    public float getScore() {
        return 0;
    }
}
