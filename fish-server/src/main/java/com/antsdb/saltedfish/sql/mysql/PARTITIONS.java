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

import java.sql.Timestamp;
import java.util.Collections;

import com.antsdb.saltedfish.sql.vdm.CursorMeta;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.sql.vdm.ViewMaker;
import com.antsdb.saltedfish.util.CursorUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class PARTITIONS extends ViewMaker {
    private static final CursorMeta META = CursorUtil.toMeta(Line.class);
    
    public static class Line {
        public String TABLE_CATALOG;
        public String TABLE_SCHEMA;
        public String TABLE_NAME;
        public String PARTITION_NAME;
        public String SUBPARTITION_NAME;
        public Long PARTITION_ORDINAL_POSITION;
        public Long SUBPARTITION_ORDINAL_POSITION;
        public String PARTITION_METHOD;
        public String SUBPARTITION_METHOD;
        public String PARTITION_EXPRESSION;
        public String SUBPARTITION_EXPRESSION;
        public String PARTITION_DESCRIPTION;
        public Long TABLE_ROWS;
        public Long AVG_ROW_LENGTH;
        public Long DATA_LENGTH;
        public Long MAX_DATA_LENGTH;
        public Long INDEX_LENGTH;
        public Long DATA_FREE;
        public Timestamp CREATE_TIME;
        public Timestamp UPDATE_TIME;
        public Timestamp CHECK_TIME;
        public Long CHECKSUM;
        public String PARTITION_COMMENT;
        public String NODEGROUP;
        public String TABLESPACE_NAME;
    }

    public PARTITIONS() {
        super(META);
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        return CursorUtil.toCursor(META, Collections.emptyList());
    }
}
