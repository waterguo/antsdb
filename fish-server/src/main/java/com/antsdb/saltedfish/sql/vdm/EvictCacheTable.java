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

import com.antsdb.saltedfish.minke.MinkeCache;
import com.antsdb.saltedfish.minke.MinkeCacheTable;
import com.antsdb.saltedfish.nosql.StorageEngine;
import com.antsdb.saltedfish.sql.OrcaException;

/**
 * 
 * @author *-xguo0<@
 */
public class EvictCacheTable extends Statement {
    private int tableId;

    public EvictCacheTable(int tableId) {
        this.tableId = tableId;
    }
    
    @SuppressWarnings("resource")
    @Override
    public Object run(VdmContext ctx, Parameters params) {
        StorageEngine storage = ctx.getHumpback().getStorageEngine();
        if (!(storage instanceof MinkeCache)) {
            throw new OrcaException("cache is not enabled");
        }
        MinkeCache cache = (MinkeCache)storage;
        MinkeCacheTable mctable = (MinkeCacheTable)cache.getTable(this.tableId);
        if (mctable == null) {
            throw new OrcaException("table {} is not found", tableId);
        }
        if (this.tableId < 0) {
            throw new OrcaException("can't evict pages from temporary table");
        }
        mctable.evictAllPages();
        return null;
    }

}
