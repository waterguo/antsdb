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

import static com.antsdb.saltedfish.util.UberFormatter.*;

import java.util.Map;

import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class LRUEvictor {
    static final Logger _log = UberUtil.getThisLogger();
    static final long GB = 1024 * 1024 * 1024;
    private MinkeCache cache;
    private int target;

    public LRUEvictor(MinkeCache cache, int target) {
        this.cache = cache;
        this.target = target;
    }
    
    public int run() {
        int count = 0;
        int bucketSize = getBucketSize();
        if (bucketSize <= 0) {
            return 0;
        }
        EvictionBucket bucket = new EvictionBucket(bucketSize);
        for (MinkeTable mtable:this.cache.minke.tableById.values()) {
            for (MinkePage page:mtable.getPages()) {
                bucket.add(mtable, page);
            }
        }
        _log.debug("{} pages in bucket", bucket.getResult().size());
        for (MinkePage page:bucket.getResult()) {
            if (page.copyLastAccess != page.lastAccess.get()) {
                _log.debug("release page {} from evition {} {}", 
                        hex(page.id), 
                        page.copyLastAccess, 
                        page.lastAccess.get());
                continue;
            }
            MinkeTable table = (MinkeTable)this.cache.minke.getTable(page.tableId);
            if (!table.deletePage(page)) {
                printDebug(page);
                continue;
            }
            count++;
        }
        if (count != bucket.getResult().size()) {
            
        }
        _log.debug("{} pages have been evicted", count);
        return count;
    }
    
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

    int getBucketSize() {
        int nUsedPage = this.cache.minke.getUsedPageCount();
        int nTotalPage = this.cache.minke.getMaxPages();
        int result = nUsedPage - nTotalPage * (100 - this.target) / 100;
        return result;
    }
}
