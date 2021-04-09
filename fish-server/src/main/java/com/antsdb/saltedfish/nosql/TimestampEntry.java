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

import java.time.Instant;
import java.time.ZoneId;

import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.nosql.Gobbler.EntryType;

/**
 * 
 * @author *-xguo0<@
 */
public class TimestampEntry extends LogEntry {
    
    TimestampEntry(SpaceManager sm, long time) {
        super(sm, 8);
        setTimestamp(time);
        finish(EntryType.TIMESTAMP);
    }
    
    protected TimestampEntry(long sp, long addr) {
        super(sp, addr);
    }
    
    private void setTimestamp(long value) {
        Unsafe.putLong(this.addr + OFFSET_TRX_ID, value);
    }

    public long getTimestamp() {
        return Unsafe.getLong(this.addr + OFFSET_TRX_ID);
    }
    
    @Override
    public String toString() {
        long ts = getTimestamp();
        return Instant.ofEpochMilli(ts).atZone(ZoneId.systemDefault()).toLocalDateTime().toString();
    }
}

