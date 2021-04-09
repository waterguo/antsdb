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
package com.antsdb.saltedfish.minke;

import org.slf4j.Logger;

import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class ClearEvictor {
    static final Logger _log = UberUtil.getThisLogger();
    
    private MinkeCache cache;

    public ClearEvictor(MinkeCache cache) {
        this.cache = cache;
    }
    
    public int run() {
        int count = 0;
        for (MinkeCacheTable table:this.cache.tableById.values()) {
            MinkeTable mtable = table.mtable;
            if (table.getId() < 0) {
                // dont evict temporary table
                continue;
            }
            for (MinkePage page:mtable.getPages()) {
                if (mtable.deletePage(page)) {
                    count++;
                }
            }
        }
        _log.debug("{} pages have been evicted", count);
        return count;
    }
}
