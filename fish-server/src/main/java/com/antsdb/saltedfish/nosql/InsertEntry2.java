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

import com.antsdb.saltedfish.cpp.BluntHeap;
import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.nosql.Gobbler.EntryType;

/**
 * 
 * @author *-xguo0<@
 */
public final class InsertEntry2 extends RowUpdateEntry2 {
    public InsertEntry2(SpaceManager sm, int sessionId, VaporizingRow row, int tableId) {
        super(sm, sessionId, row.getSize(), tableId);
        Row.from(getRowPointer(), row);
        finish(EntryType.INSERT2);
    }
    
    public InsertEntry2(BluntHeap heap, int sessionId, Row row, int tableId) {
        super(heap, sessionId, row.getLength(), tableId);
        Unsafe.copyMemory(row.getAddress(), getRowPointer(), row.getLength());
        finish(EntryType.INSERT2);
    }
    
    public InsertEntry2(long sp, long addr) {
        super(sp, addr);
    }

}

