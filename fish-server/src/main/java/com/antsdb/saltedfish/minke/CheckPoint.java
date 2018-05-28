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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.nosql.MemoryMappedFile;

/**
 * 
 * @author *-xguo0<@
 */
class CheckPoint implements Closeable {
    static final int OFFSET_OPEN = 0;
    static final int OFFSET_LOG_SPAN_END = 8;
    
    private File file;
    private MemoryMappedFile mmf;
    private long addr;
    private boolean isProperlyClosed;

    CheckPoint(File file) {
        this.file = file;
    }
    
    void open() throws IOException {
        this.mmf = new MemoryMappedFile(file, 512, "rw");
        this.addr = mmf.getAddress();
        this.isProperlyClosed = !getOpenFlag(); 
        setOpenFlag(true);
    }

    @Override
    public void close() throws IOException {
        setOpenFlag(false);
        this.mmf.close();
    }
    
    private void setOpenFlag(boolean value) {
        Unsafe.putByte(this.addr + OFFSET_OPEN, (byte)(value ? 1 : 0));
    }
    
    private boolean getOpenFlag() {
        return Unsafe.getByte(this.addr + OFFSET_OPEN) == 0 ? false : true;
    }
    
    boolean isProperlyClosedLastTime() {
        return this.isProperlyClosed;
    }
    
    public long getEndSP() {
        return Unsafe.getLong(this.addr + OFFSET_LOG_SPAN_END);
    }
    
    public void setEndSP(long sp) {
        Unsafe.putLong(this.addr + OFFSET_LOG_SPAN_END, sp);
    }
}
