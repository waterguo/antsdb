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

import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.nosql.Gobbler.EntryType;

/**
 * 
 * @author *-xguo0<@
 */
public final class DeleteRowEntry2 extends RowUpdateEntry2 {
    DeleteRowEntry2(SpaceManager sm, int sessionId, long pRow, int tableId, long version) {
        super(sm, sessionId, Row.getLength(pRow), tableId);
        long pData = getRowPointer();
        Unsafe.copyMemory(pRow, pData, Row.getLength(pRow));
        Row.setVersion(pData, version);
        finish(EntryType.DELETE_ROW2);
    }
    
    public DeleteRowEntry2(long sp, long addr) {
        super(sp, addr);
    }
    
    public long getKeyAddress() {
        long pRow = getRowPointer();
        return Row.getKeyAddress(pRow);
    }
}

