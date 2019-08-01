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
class EvictableTable extends EvictableObject {
    static final Logger _log = UberUtil.getThisLogger();
    
    private MinkeCacheTable table;
    private long lastAccessTime;

    EvictableTable(MinkeCacheTable table) {
        this.table = table;
        for (MinkePage i:table.mtable.getPages()) {
            this.lastAccessTime = Math.max(this.lastAccessTime, i.getLastRead());
        }
    }
    
    @Override
    long getLastAccessTime() {
        return this.lastAccessTime;
    }

    @Override
    int evict() {
        return this.table.evictAllPages();
    }
}
