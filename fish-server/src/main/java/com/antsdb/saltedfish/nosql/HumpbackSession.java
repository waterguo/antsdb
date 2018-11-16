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

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicInteger;

import com.antsdb.saltedfish.util.UberTime;

/**
 * represents a humpback session
 *  
 * @author *-xguo0<@
 */
public final class HumpbackSession implements Closeable{
    static AtomicInteger _nextId = new AtomicInteger(1);
    
    long ts;
    int id = 0;
    
    public HumpbackSession() {
        // make sure id > 0
        for (;this.id <= 0;) {
            this.id = _nextId.getAndIncrement();
            if (this.id < 0) {
                _nextId.compareAndSet(this.id, 1);
            }
        }
    }
    
    public HumpbackSession open() {
        ts = UberTime.getTime();
        return this;
    }
    
    @Override
    public void close() {
        this.ts = 0;
    }

    public long getOpenTime() {
        return this.ts;
    }

    public int getId() {
        return this.id;
    }
    
    @Override
    public String toString() {
        return "hsession: " + this.id;
    }
}
