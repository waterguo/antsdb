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
package com.antsdb.saltedfish.cpp;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class Heap2 {
    List<ByteBuffer> buffers = new ArrayList<>();
    int current = 0;
    
    public Heap2() {
        buffers.add(MemoryManager.alloc(FlexibleHeap.DEFAULT_SIZE));
    }
    /**
     * 
     * @param size
     * @return address/pointer of the allocated address
     */
    public final long alloc(int size) {
        if (this.getCurrent().remaining() < (size)) {
            buffers.add(MemoryManager.alloc(FlexibleHeap.DEFAULT_SIZE));
            this.current++;
        }
        ByteBuffer buf = getCurrent();
        int pos = buf.position();
        long address = ((long)this.current) << 32;
        address += pos;
        buf.position(pos + size);
        return address;
    }

    private ByteBuffer getCurrent() {
        return buffers.get(this.current);
    }
}
