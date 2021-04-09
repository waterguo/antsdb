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
package com.antsdb.saltedfish.sql.vdm;

import java.util.concurrent.atomic.AtomicLong;

import com.antsdb.saltedfish.nosql.RowIterator;

/**
 * 
 * @author *-xguo0<@
 */
public final class HumpbackCursor extends Cursor {
    private RowIterator iter;
    private boolean isClosed = false;
    private AtomicLong counter;

    public HumpbackCursor(CursorMeta meta, RowIterator iter, AtomicLong counter) {
        super(meta);
        this.counter = counter;
        this.iter = iter;
    }

    @Override
    public long next() {
        if (isClosed) {
            return 0;
        }
        boolean hasNext = iter.next();
        if (!hasNext) {
            return 0;
        }
        long pRow = iter.getRowPointer();
        this.counter.incrementAndGet();
        return pRow;
    }

    @Override
    public void close() {
        this.isClosed = true;
    }
}
