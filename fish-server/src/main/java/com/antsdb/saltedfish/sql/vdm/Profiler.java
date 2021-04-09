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
package com.antsdb.saltedfish.sql.vdm;

import java.util.HashMap;
import java.util.Map;

/**
 * 
 * @author *-xguo0<@
 */
public final class Profiler {
    private Map<Integer, CursorStats> profilers = new HashMap<>();
    
    public ProfileRecord getRecord(int id) {
        CursorStats profiler = this.profilers.get(id);
        return profiler != null ? profiler.getRecord() : null;
    }

    public CursorStats getStats(int makerId) {
        CursorStats result = this.profilers.get(makerId);
        if (result == null) {
            result = new CursorStats();
            this.profilers.put(makerId, result);
        }
        return result;
    }

}
