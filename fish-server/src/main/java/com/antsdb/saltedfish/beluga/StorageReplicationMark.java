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
package com.antsdb.saltedfish.beluga;

import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.nosql.Gobbler.EntryType;
import com.antsdb.saltedfish.nosql.LogEntry;

/**
 * 
 * @author *-xguo0<@
 */
public class StorageReplicationMark extends LogEntry {
    private static final int OFFSET_LP = HEADER_SIZE;
    
    public StorageReplicationMark(long addr) {
        super(0, addr);
    }
    
    public static StorageReplicationMark alloc(Heap heap) {
        LogEntry entry = new LogEntry(heap, HEADER_SIZE + 8);
        entry.setMagic();
        entry.setType(EntryType.OTHER);
        return new StorageReplicationMark(entry.getAddress());
    }
    
    public void setLogPointer(long value) {
        Unsafe.putLong(this.addr + OFFSET_LP, value);
    }
    
    public long getLogPointer() {
        return Unsafe.getLong(this.addr + OFFSET_LP);
    }
}
