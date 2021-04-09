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

import org.apache.commons.codec.Charsets;

import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.nosql.Gobbler.EntryType;

/**
 * 
 * @author *-xguo0<@
 */
public final class DdlEntry extends LogEntry {
    protected final static int OFFSET_SESSION = 6;
    protected final static int OFFSET_DDL = 0xa;
    
    static DdlEntry alloc(SpaceManager sm, int sessionId, String message) {
        byte[] bytes = message.getBytes(Charsets.UTF_8);
        DdlEntry entry = new DdlEntry(sm, bytes.length);
        entry.setSessionId(sessionId);
        Unsafe.putBytes(entry.addr + OFFSET_DDL, bytes);
        entry.finish(EntryType.DDL);
        return entry;
    }
    
    private DdlEntry(SpaceManager sm, int size) {
        super(sm, OFFSET_DDL - LogEntry.HEADER_SIZE + size);
    }
    
    DdlEntry(long sp, long addr) {
        super(sp, addr);
    }

    public static int getHeaderSize() {
        return OFFSET_DDL;
    }
    
    public void setSessionId(int value) {
        Unsafe.putInt(this.addr + OFFSET_SESSION, value);
    }
    
    public int getSessionId() {
        return Unsafe.getInt(this.addr + OFFSET_SESSION);
    }
    
    public String getDdl() {
        int size = getSize() - OFFSET_DDL + LogEntry.HEADER_SIZE;
        byte[] bytes = new byte[size];
        Unsafe.getBytes(this.addr + OFFSET_DDL, bytes);
        return new String(bytes, Charsets.UTF_8);
    }
}

