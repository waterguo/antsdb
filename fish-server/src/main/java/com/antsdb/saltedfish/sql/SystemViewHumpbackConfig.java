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

import java.util.HashMap;
import java.util.Map;

import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.RowIterator;
import com.antsdb.saltedfish.nosql.SlowRow;
import com.antsdb.saltedfish.nosql.SysConfigRow;
import com.antsdb.saltedfish.sql.vdm.VdmContext;

/**
 * 
 * @author *-xguo0<@
 */
public class SystemViewHumpbackConfig extends PropertyBasedView {

    @Override
    public Map<String, Object> getProperties(VdmContext ctx) {
        Map<String, Object> result = new HashMap<>();
        GTable sysconfig = ctx.getOrca().getHumpback().getTable(Humpback.SYSCONFIG_TABLE_ID);
        for (RowIterator i=sysconfig.scan(0, Long.MAX_VALUE, true);i.next();) {
            SysConfigRow row = new SysConfigRow(SlowRow.from(i.getRow()));
            String key = row.getKey();
            String value = row.getVale();
            if (key.contains("password")) {
                value = "********";
            }
            result.put(key, value);
        }
        return result;
    }
}
