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

import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.vdm.Cursor;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.sql.vdm.ViewMaker;
import com.antsdb.saltedfish.util.CursorUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class TRIGGERS extends ViewMaker {

    Orca orca;

    public static class Item {
        public String TRIGGER_CATALOG;
        public String TRIGGER_SCHEMA;
        public String TRIGGER_NAME;
        public String EVENT_MANIPULATION;
        public String EVENT_OBJECT_CATALOG;
        public String EVENT_OBJECT_SCHEMA;
        public String EVENT_OBJECT_TABLE;
        public Long ACTION_ORDER;
        public String ACTION_CONDITION;
        public String ACTION_STATEMENT;
        public String ACTION_ORIENTATION;
        public String ACTION_TIMING;
        public String ACTION_REFERENCE_OLD_TABLE;
        public String ACTION_REFERENCE_NEW_TABLE;
        public String ACTION_REFERENCE_OLD_ROW;
        public String ACTION_REFERENCE_NEW_ROW;
        public Timestamp CREATED;
        public String SQL_MODE;
        public String DEFINER;
        public String CHARACTER_SET_CLIENT;
        public String COLLATION_CONNECTION;
        public String DATABASE_COLLATION;
    }

    public TRIGGERS(Orca orca) {
        super(CursorUtil.toMeta(Item.class));
        this.orca = orca;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        Cursor c = CursorUtil.toCursor(meta, Collections.emptyList());
        return c;
    }
}
