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
package com.antsdb.saltedfish.sql.mysql;

import java.util.Collections;

import com.antsdb.saltedfish.sql.vdm.CursorMeta;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.sql.vdm.ViewMaker;
import com.antsdb.saltedfish.util.CursorUtil;

/**
 * https://mariadb.com/kb/en/library/information-schema-statistics-table/
 * 
 * @author *-xguo0<@
 */
public class STATISTICS extends ViewMaker {
    private static final CursorMeta META = CursorUtil.toMeta(Line.class);
    
    public static class Line {
        public String TABLE_CATALOG;
        public String TABLE_SCHEMA;
        public String TABLE_NAME;
        public Long NON_UNIQUE;
        public String INDEX_SCHEMA;
        public String INDEX_NAME;
        public Long SEQ_IN_INDEX;
        public String COLUMN_NAME;
        public String COLLATION;
        public Long CARDINALITY;
        public Long SUB_PART;
        public String PACKED;
        public String NULLABLE;
        public String INDEX_TYPE;
        public String COMMENT;
    }
    
    public STATISTICS() {
        super(META);
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        return CursorUtil.toCursor(META, Collections.emptyList());
    }
}
