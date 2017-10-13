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
package com.antsdb.saltedfish.sql.vdm;

import java.util.concurrent.atomic.AtomicLong;

import com.antsdb.saltedfish.cpp.FishBool;
import com.antsdb.saltedfish.cpp.Heap;

public class FilteredCursor extends CursorWithHeap {
    Cursor upstream;
    Operator filter;
    VdmContext ctx;
    Parameters params;
    boolean allFiltered = true;
    boolean outer;
	private AtomicLong counter;
    
    public FilteredCursor(
    		VdmContext ctx, 
    		Parameters params, 
    		Cursor upstream, 
    		Operator expr, 
    		boolean outer, 
    		AtomicLong counter) {
        super(upstream.getMetadata());
        this.upstream = upstream;
        this.filter = expr;
        this.ctx = ctx;
        this.params = params;
        this.outer = outer;
        this.counter = counter;
    }

    @Override
    public long next() {
        for (;;) {
            long pRecord = this.upstream.next();
            if (pRecord == 0) {
                return 0;
            }
            this.counter.incrementAndGet();
            
            // evaluate the condition
            
            Heap heap = newHeap();
            long pBool = this.filter.eval(ctx, heap, params, pRecord);
            if (pBool == 0) {
            	continue;
            }
            boolean b = FishBool.get(null, pBool);
            if (b) {
                this.allFiltered = false;
                return pRecord;
            }
        }
    }

    @Override
    public void close() {
        super.close();
        this.upstream.close();
    }

    @Override
    public String toString() {
        return this.upstream.toString();
    }

}
