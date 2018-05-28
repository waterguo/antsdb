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
import com.antsdb.saltedfish.sql.vdm.ViewMaker;
import com.antsdb.saltedfish.util.CursorUtil;

/**
 * https://mariadb.com/kb/en/library/information-schema-key_column_usage-table/
 * 
 * @author *-xguo0<@
 */
public class KEY_COLUMN_USAGE extends ViewMaker {
    private static final CursorMeta META = CursorUtil.toMeta(Line.class);
    
    public static class Line {
        public String CONSTRAINT_CATALOG;
        public String CONSTRAINT_SCHEMA;
        public String CONSTRAINT_NAME;
        public String TABLE_CATALOG;
        public String TABLE_SCHEMA;
        public String TABLE_NAME;
        public String COLUMN_NAME;
        public Long ORDINAL_POSITION;
        public Long POSITION_IN_UNIQUE_CONSTRAINT;
        public String REFERENCED_TABLE_SCHEMA;
        public String REFERENCED_TABLE_NAME;
        public String REFERENCED_COLUMN_NAME;
    }

    public KEY_COLUMN_USAGE() {
        super(META);
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        return CursorUtil.toCursor(META, Collections.emptyList());
    }
}
