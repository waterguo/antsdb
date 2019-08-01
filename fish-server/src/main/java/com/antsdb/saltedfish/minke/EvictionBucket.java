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

import java.util.Collection;
import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * 
 * @author *-xguo0<@
 */
class EvictionBucket {
    
    private int size;
    private PriorityQueue<EvictableObject> set = new PriorityQueue<>(new MyComparator());
    private long min = Long.MAX_VALUE;
    private long max;
    
    static class MyComparator implements Comparator<EvictableObject>{
        @Override
        public int compare(EvictableObject x, EvictableObject y) {
            return -Long.compare(x.getLastAccessTime(), y.getLastAccessTime());
        }
    }
    
    EvictionBucket(int size) {
        this.size = size;
    }
    
    Collection<EvictableObject> getResult() {
        return this.set;
    }

    public void add(EvictableObject evictable) {
        long lastAccess = evictable.getLastAccessTime();
        this.min = Math.min(this.min, lastAccess);
        this.max = Math.max(this.max, lastAccess);
        this.set.add(evictable);
        while (set.size() > this.size) {
            this.set.poll();
        }
    }
}
