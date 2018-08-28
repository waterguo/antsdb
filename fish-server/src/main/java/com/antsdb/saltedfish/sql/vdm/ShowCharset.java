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

import com.antsdb.saltedfish.sql.planner.SortKey;
import com.antsdb.saltedfish.util.CursorUtil;

public class ShowCharset extends ViewMaker {
    public static class Line {
        Line(String name, String description, String collate, long maxlen) {
            this.CHARACTER_SET_NAME = name;
            this.DEFAULT_COLLATE_NAME = collate;
            this.DESCRIPTION = description;
            this.MAXLEN = maxlen;
        }
        
        // Charset
        public String CHARACTER_SET_NAME;
        // Default collation
        public String DESCRIPTION;
        // Description
        public String DEFAULT_COLLATE_NAME;
        // Maxlen
        public Long MAXLEN;
    }
    
    public ShowCharset() {
        super(CursorUtil.toMeta(Line.class));
    }

    @Override
    public CursorMeta getCursorMeta() {
        return this.meta;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        // wgu0: hard coded for now
        List<Line> result = new ArrayList<>();
        result.add(new Line("ascii", "US ASCII", "ascii_general_ci", 1));
        result.add(new Line("utf8", "UTF-8 Unicode", "utf8_general_ci", 3));
        result.add(new Line("utf8mb4", "UTF-8 Unicode", "utf8mb4_general_ci", 4));
        Cursor c = CursorUtil.toCursor(getCursorMeta(), result);
        return c;
    }

    @Override
    public boolean setSortingOrder(List<SortKey> order) {
        return false;
    }
}
