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
public final class MessageEntry extends LogEntry {
    protected final static int OFFSET_MESSAGE = 6;
    
    static MessageEntry alloc(SpaceManager sm, String message) {
        byte[] bytes = message.getBytes(Charsets.UTF_8);
        MessageEntry entry = new MessageEntry(sm, bytes.length);
        long p = entry.addr;
        for (int i=0; i<bytes.length; i++) {
            Unsafe.putByte(p + OFFSET_MESSAGE + i, bytes[i]);
        }
        entry.finish(EntryType.MESSAGE);
        return entry;
    }
    
    private MessageEntry(SpaceManager sm, int size) {
        super(sm, size);
    }
    
    MessageEntry(long sp, long addr) {
        super(sp, addr);
    }

    public String getMessage() {
        int size = getSize();
        byte[] bytes = new byte[size];
        for (int i=0; i<size; i++) {
            bytes[i] = Unsafe.getByte(this.addr + OFFSET_MESSAGE + i);
        }
        return new String(bytes, Charsets.UTF_8);
    }
}

