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

import java.sql.Timestamp;
import java.util.Collections;

import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.vdm.Cursor;
import com.antsdb.saltedfish.sql.vdm.CursorMeta;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.sql.vdm.ViewMaker;
import com.antsdb.saltedfish.util.CursorUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class EVENTS extends ViewMaker {

    Orca orca;
    CursorMeta meta;

    public static class Item {
        public String EVENT_CATALOG;
        public String EVENT_SCHEMA;
        public String EVENT_NAME;
        public String DEFINER;
        public String TIME_ZONE;
        public String EVENT_BODY;
        public byte[] EVENT_DEFINITION;
        public String EVENT_TYPE;
        public Timestamp EXECUTE_AT;
        public String INTERVAL_VALUE;
        public String INTERVAL_FIELD;
        public String SQL_MODE;
        public Timestamp STARTS;
        public Timestamp ENDS;
        public String STATUS;
        public String ON_COMPLETION;
        public Timestamp CREATED;
        public Timestamp LAST_ALTERED;
        public Timestamp LAST_EXECUTED;
        public String EVENT_COMMENT;
        public Long ORIGINATOR;
        public String CHARACTER_SET_CLIENT;
        public String COLLATION_CONNECTION;
        public String DATABASE_COLLATION;
    }

    public EVENTS(Orca orca) {
        super(CursorUtil.toMeta(Item.class));
        this.orca = orca;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        Cursor c = CursorUtil.toCursor(meta, Collections.emptyList());
        return c;
    }
}
