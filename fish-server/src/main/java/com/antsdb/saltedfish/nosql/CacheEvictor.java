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
package com.antsdb.saltedfish.nosql;

import org.slf4j.Logger;

import com.antsdb.saltedfish.minke.LRUEvictor;
import com.antsdb.saltedfish.minke.MinkeCache;
import com.antsdb.saltedfish.util.SizeConstants;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class CacheEvictor implements Runnable{
    static final Logger _log = UberUtil.getThisLogger();
    /** number of bytes from the cache supposed to be free */
    static final long EVICTION_TARGET=SizeConstants.gb(10);
    
    private MinkeCache cache;
    private LRUEvictor evictor;
    
    CacheEvictor(MinkeCache cache) {
        this(cache, EVICTION_TARGET);
    }
    
    CacheEvictor(MinkeCache cache, long target) {
        this.cache = cache;
        this.evictor = new LRUEvictor(cache, target);
    }

    
    @Override
    public void run() {
        try {
            evict();
        }
        catch (Exception x) {
            _log.error("failed to evict cache", x);
        }
    }

    public synchronized int evict() throws Exception {
        // we want 20% of the cache to be available for new updates
        
        int result = 0;
        if (cache.getUsage() >= (100 - EVICTION_TARGET)) {
            result = this.evictor.run();
        }
        
        // get rid of zombie pages
        
        this.cache.checkpointIfNeccessary();
        return result;
    }
}
