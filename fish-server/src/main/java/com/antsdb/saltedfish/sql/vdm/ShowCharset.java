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
import com.antsdb.saltedfish.util.MysqlColumnMeta;

public class ShowCharset extends View {
    public static class Line {
        Line(String name, String description, String collate, long maxlen) {
            this.Charset = name;
            this.Default_collation = collate;
            this.Description = description;
            this.Maxlen = maxlen;
        }
        
        @MysqlColumnMeta(column="CHARACTER_SET_NAME")
        public String Charset;
        @MysqlColumnMeta(column="DESCRIPTION")
        public String Description;
        @MysqlColumnMeta(column="DEFAULT_COLLATE_NAME")
        public String Default_collation;
        @MysqlColumnMeta(column="MAXLEN")
        public Long Maxlen;
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
