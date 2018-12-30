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
package com.antsdb.saltedfish.beluga;

import java.util.HashMap;
import java.util.Map;

import com.antsdb.saltedfish.sql.PropertyBasedView;
import com.antsdb.saltedfish.sql.vdm.VdmContext;

/**
 * 
 * @author *-xguo0<@
 */
public class SystemViewSlaveWarmerInfo extends PropertyBasedView {
    @Override
    public Map<String, Object> getProperties(VdmContext ctx) {
        Map<String, Object> result = new HashMap<>();
        Pod pod = ctx.getOrca().getBelugaPod();
        if (pod != null) {
            SlaveWarmer warmer = pod.getWarmer();
            if (warmer != null) {
                result.put("total hits", warmer.getTotalHits());
                result.put("total errors", warmer.totalError);
                result.put("hits in last 5 seconds", warmer.getRecentHits());
            }
        }
        return result;
    }
}
