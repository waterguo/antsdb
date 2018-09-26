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

import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.sql.vdm.View;
import com.antsdb.saltedfish.util.CursorUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class ShowFunction extends View {
    public static class Line {
        public String ROUTINE_SCHEMA;
        public String ROUTINE_NAME;
        public String ROUTINE_TYPE;
        public String DEFINER;
        public Timestamp LAST_ALTERED;
        public Timestamp CREATED;
        public String SECURITY_TYPE;
        public String ROUTINE_COMMENT;
        public String CHARACTER_SET_CLIENT;
        public String COLLATION_CONNECTION;
        public String DATABASE_COLLATION;
    }

    public ShowFunction() {
        super(CursorUtil.toMeta(Line.class));
    }

    @Override
    public Object run(VdmContext ctx, Parameters notused, long pMaster) {
        return CursorUtil.toCursor(getCursorMeta(), Collections.emptyList());
    }
}
