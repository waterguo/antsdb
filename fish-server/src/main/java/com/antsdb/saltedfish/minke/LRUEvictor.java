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
public class LRUEvictor {
    static final Logger _log = UberUtil.getThisLogger();
    static final long GB = 1024 * 1024 * 1024;
    private MinkeCache cache;
    private long target;

    /**
     * 
     * @param cache
     * @param target number of bytes to be kept as free 
     */
    public LRUEvictor(MinkeCache cache, long target) {
        if (cache.getMinke().size < target * 2) {
            throw new IllegalArgumentException("cache size must be larger than " + target * 2);
        }
        this.cache = cache;
        this.target = target;
    }
    
    public int run() {
        int count = 0;
        int bucketSize = getBucketSize();
        if (bucketSize <= 0) {
            return 0;
        }
        
        // add evictable objects to bucket
        _log.debug("page counts: current={} free={} used={} garbage={} zombie={}",
                this.cache.minke.getCurrentPageCount(), 
                this.cache.minke.getFreePageCount(),
                this.cache.minke.getUsedPageCount(),
                this.cache.minke.getGarbagePageCount(),
                this.cache.minke.getZombiePageCount());
        EvictionBucket bucket = new EvictionBucket(bucketSize);
        for (MinkeCacheTable mctable:this.cache.tableById.values()) {
            if (mctable.getId() < 0x100) {
                // skip system or temporary tables
                continue;
            }
            if (mctable.isFullCache() && mctable.mtable.getPageCount()>0) {
                // we want to evict table as whole if it is fully cached
                bucket.add(new EvictableTable(mctable));
            }
            else {
                MinkeTable mtable = mctable.mtable;
                for (MinkePage page:mtable.getPages()) {
                    bucket.add(new EvictablePage(mctable, page));
                }
            }
        }
        
        // evict pages 
        _log.debug("{} pages in bucket", bucket.getResult().size());
        for (EvictableObject i:bucket.getResult()) {
            count += i.evict();
            if (count >= bucketSize) {
                break;
            }
        }
        _log.debug("{} pages have been evicted", count);
        _log.debug("page counts: current={} free={} used={} garbage={} zombie={}",
                this.cache.minke.getCurrentPageCount(), 
                this.cache.minke.getFreePageCount(),
                this.cache.minke.getUsedPageCount(),
                this.cache.minke.getGarbagePageCount(),
                this.cache.minke.getZombiePageCount());
        return count;
    }
    
    int getBucketSize() {
        long pageSize = this.cache.minke.getPageSize();
        long freeBytes = this.cache.getMinke().getFreePageCount() * pageSize;
        return freeBytes>=this.target ? 0 : (int)((this.target-freeBytes) / pageSize);
    }
}
