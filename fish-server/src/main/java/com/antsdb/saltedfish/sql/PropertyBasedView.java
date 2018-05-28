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
package com.antsdb.saltedfish.sql;

import java.util.Map;
import java.util.Properties;

import com.antsdb.saltedfish.sql.vdm.Cursor;
import com.antsdb.saltedfish.sql.vdm.EmptyCursor;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.sql.vdm.ViewMaker;
import com.antsdb.saltedfish.util.CursorUtil;

/**
 * 
 * @author *-xguo0<@
 */
public abstract class PropertyBasedView extends ViewMaker {
    public abstract Map<String, Object> getProperties();
    
    public PropertyBasedView() {
        super(CursorUtil.toMeta(Properties.class));
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        Map<String, Object> props = getProperties();
        Cursor c;
        if (props != null) {
            c = CursorUtil.toCursor(meta, props);
        }
        else {
            c = new EmptyCursor(meta);
        }
        return c;
    }
}
