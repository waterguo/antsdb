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

import com.antsdb.saltedfish.minke.Minke;
import com.antsdb.saltedfish.minke.MinkeCache;
import com.antsdb.saltedfish.minke.MinkeCacheTable;
import com.antsdb.saltedfish.minke.MinkePage;
import com.antsdb.saltedfish.minke.PageState;
import com.antsdb.saltedfish.nosql.StorageEngine;
import com.antsdb.saltedfish.sql.OrcaException;

/**
 * 
 * @author *-xguo0<@
 */
public class EvictCachePage extends Statement {
    
    private int pageId;

    public EvictCachePage(int pageId) {
        this.pageId = pageId;
    }
    
    @SuppressWarnings("resource")
    @Override
    public Object run(VdmContext ctx, Parameters params) {
        StorageEngine storage = ctx.getHumpback().getStorageEngine();
        if (!(storage instanceof MinkeCache)) {
            throw new OrcaException("cache is not enabled");
        }
        MinkeCache cache = (MinkeCache)storage;
        Minke minke = cache.getMinke();
        MinkePage page = minke.getPage(this.pageId);
        if (page == null) {
            throw new OrcaException("page {} is not found", this.pageId);
        }
        if ((page.getState() != PageState.ACTIVE) && (page.getState() != PageState.CARBONFREEZED)) {
            throw new OrcaException("page {} is not active currently", this.pageId);
        }
        int tableId = page.getTableId();
        MinkeCacheTable table = (MinkeCacheTable)cache.getTable(tableId);
        if (table == null) {
            throw new OrcaException("table {} is not found", tableId);
        }
        if (table.isFullCache()) {
            throw new OrcaException("can't evict pages from fully cached table");
        }
        boolean success = table.evictPage(page) == 1;
        if (!success) {
            throw new OrcaException("unable to evict page {} from table {}", this.pageId, tableId);
        }
        return null;
    }
    /* old debug code
    private String safeGetKeyString(long pKey) {
        try {
            return KeyBytes.toString(pKey);
        }
        catch (Exception x) {
            return x.getMessage();
        }
    }
    
    private void printDebug(MinkePage page) {
        MinkeTable table = (MinkeTable)this.cache.minke.getTable(page.tableId);
        _log.debug("unable to delete page {} {} from table {}", hex(page.id), hex(page.pStartKey), table.tableId);
        MinkePage that = table.pages.get(page.getStartKeyPointer());
        if (that == null) {
            _log.error("that = null");
        }
        else {
            _log.error("that page {} {} {}", hex(that.id), hex(that.pStartKey), safeGetKeyString(that.pStartKey));
        }
        for (Map.Entry<KeyBytes, MinkePage> i:table.pages.entrySet()) {
            long pKey = i.getKey().getAddress();
            MinkePage ii = i.getValue();
            if (ii == page) {
                _log.debug("{} {} {} {} {}", 
                        hex(ii.id), 
                        hex(pKey), 
                        hex(ii.getStartKeyPointer()), 
                        hex(ii.getStartKey().getAddress()),
                        safeGetKeyString(pKey));
            }
        }
    }
    */
}
