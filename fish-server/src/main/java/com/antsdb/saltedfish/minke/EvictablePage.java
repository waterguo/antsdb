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
class EvictablePage extends EvictableObject {
    static final Logger _log = UberUtil.getThisLogger();

    private MinkePage page;
    private long lastAccessTime;
    private MinkeCacheTable table;

    EvictablePage(MinkeCacheTable mtable, MinkePage page) {
        this.table = mtable;
        this.page = page;
        this.lastAccessTime = page.getLastRead();
    }

    @Override
    long getLastAccessTime() {
        return this.lastAccessTime;
    }

    @Override
    int evict() {
        synchronized(this.table) {
            int result = this.table.mtable.deletePage(this.page) ? 1 : 0;
            _log.debug("page {} of table {} is evicted {} {}", 
                    page.id,
                    this.table.getId(),
                    page.copyLastAccess, 
                    page.lastAccess.get());
            return result;
        }
    }
}
