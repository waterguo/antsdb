/*-------------------------------------------------------------------------------------------------
 _______ __   _ _______ _______ ______  ______
 |_____| | \  |    |    |______ |     \ |_____]
 |     | |  \_|    |    ______| |_____/ |_____]

 Copyright (c) 2016, antsdb.com and/or its affiliates. All rights reserved. *-xguo0<@

 This program is free software: you can redistribute it and/or modify it under the terms of the
 GNU Affero General Public License, version 3, as published by the Free Software Foundation.

 You should have received a copy of the GNU Affero General Public License along with this program.
 If not, see <https://www.gnu.org/licenses/agpl-3.0.txt>
-------------------------------------------------------------------------------------------------*/
package com.antsdb.saltedfish.nosql;

/**
 * 
 * @author *-xguo0<@
 */
public class LogRetentionByTime extends LogRetentionStrategy {
    private long seconds;

    public LogRetentionByTime(long seconds) {
        this.seconds = seconds;
    }

    @Override
    public boolean shouldRetain(SpaceManager sm, Space space) {
        long timestamp = System.currentTimeMillis() - space.file.lastModified();
        return timestamp / 1000 <= this.seconds;
    }
    
    @Override
    public String toString() {
        return "log retention by seconds: " + this.seconds;
    }
}
