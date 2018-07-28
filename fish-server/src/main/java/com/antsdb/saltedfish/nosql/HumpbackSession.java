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

import com.antsdb.saltedfish.util.UberTime;

/**
 * represents a humpback session
 *  
 * @author *-xguo0<@
 */
public class HumpbackSession implements Closeable{
    long ts;
    
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
}
