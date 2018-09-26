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
import com.antsdb.saltedfish.sql.vdm.View;
import com.antsdb.saltedfish.util.CursorUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class ROUTINES extends View {

    Orca orca;

    public static class Item {
        public String SPECIFIC_NAME;
        public String ROUTINE_CATALOG;
        public String ROUTINE_SCHEMA;
        public String ROUTINE_NAME;
        public String ROUTINE_TYPE;
        public Integer CHARACTER_MAXIMUM_LENGTH;
        public Integer CHARACTER_OCTET_LENGTH;
        public Integer NUMERIC_PRECISION;
        public Integer NUMERIC_SCALE;
        public Integer DATETIME_PRECISION;
        public String CHARACTER_SET_NAME;
        public String COLLATION_NAME;
        public String DTD_IDENTIFIER;
        public String ROUTINE_BODY;
        public String ROUTINE_DEFINITION;
        public String EXTERNAL_NAME;
        public String EXTERNAL_LANGUAGE;
        public String PARAMETER_STYLE;
        public String IS_DETERMINSTIC;
        public String SQL_DATA_ACCESS;
        public String SQL_PATH;
        public String SECURITY_TYPE;
        public Timestamp CREATED;
        public Timestamp LAST_ALTERED;
        public String SQL_MODE;
        public String ROUTINE_COMMENT;
        public String DEFINER;
        public String CHARACTER_SET_CLIENT;
        public String COLLATION_CONNECTION;
        public String DATABASE_COLLATION;
    }

    public ROUTINES(Orca orca) {
        super(CursorUtil.toMeta(Item.class));
        this.orca = orca;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        Cursor c = CursorUtil.toCursor(meta, Collections.emptyList());
        return c;
    }
}
