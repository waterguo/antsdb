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

import com.antsdb.saltedfish.sql.vdm.Cursor;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.sql.vdm.View;
import com.antsdb.saltedfish.util.CursorUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class PerfEventsStatementsCurrent extends View {

    public static class Item {
        public long THREAD_ID;
        public long EVENT_ID;
        public long END_EVENT_ID;
        public String EVENT_NAME;
        public String SOURCE;
        public long TIMER_START;
        public long TIMER_END;
        public long TIMER_WAIT;
        public long LOCK_TIME;
        public String SQL_TEXT;
        public String DIGEST;
        public String DIGEST_TEXT;
        public String CURRENT_SCHEMA;
        public String OBJECT_TYPE;
        public String OBJECT_SCHEMA;
        public String OBJECT_NAME;
        public long OBJECT_INSTANCE_BEGIN;
        public int MYSQL_ERRNO;
        public String RETURNED_SQLSTATE;
        public String MESSAGE_TEXT;
        public long ERRORS;
        public long WARNINGS;
        public long ROWS_AFFECTED;
        public long ROWS_SENT;
        public long ROWS_EXAMINED;
        public long CREATED_TMP_DISK_TABLES;
        public long CREATED_TMP_TABLES;
        public long SELECT_FULL_JOIN;
        public long SELECT_FULL_RANGE_JOIN;
        public long SELECT_RANGE;
        public long SELECT_RANGE_CHECK;
        public long SELECT_SCAN;
        public long SORT_MERGE_PASSES;
        public long SORT_RANGE;
        public long SORT_ROWS;
        public long SORT_SCAN;
        public long NO_INDEX_USED;
        public long NO_GOOD_INDEX_USED;
        public long NESTING_EVENT_ID;
        public String NESTING_EVENT_TYPE;
    }

    public PerfEventsStatementsCurrent() {
        super(CursorUtil.toMeta(Item.class));
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        Cursor c = CursorUtil.toCursor(meta, Collections.emptyList());
        return c;
    }
}
