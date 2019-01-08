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

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.nosql.InterruptException;

public class ExprCursor extends CursorWithHeap {
    Cursor upstream;
    List<Operator> exprs;
    VdmContext ctx;
    Parameters params;
    long bufferedRecord = 0;
    boolean isEmptyCursor = true;
    private AtomicLong counter;
    private Script source;
    
    public ExprCursor(CursorMeta meta, Cursor upstream, Parameters params, AtomicLong counter) {
        super(meta);
        this.upstream = upstream;
        this.params = params;
        this.counter = counter;
    }

    @Override
    public long next() {
        if (Thread.interrupted()) {
            throw new InterruptException();
        }
        
        // buffering logic

        if (this.bufferedRecord != 0) {
            long pRecord = this.bufferedRecord;
            this.bufferedRecord = 0;
            return pRecord;
        }

        // pulling from upstream
        
        long pRecUpstream = this.upstream.next();
        if (pRecUpstream == 0) {
            return 0;
        }
        
        //  if this is group end, well it is getting complex
        
        if (Record.isGroupEnd(pRecUpstream)) {
            	if (!this.isEmptyCursor) {
                // group end record doesn't produce anything.
                resetAggregationFunctions(pRecUpstream);
                return pRecUpstream;
            }
            else {
                // except if the cursor is empty, we need to product at least 1
                // record
                this.bufferedRecord = pRecUpstream;
                long pResult = populateRecord(pRecUpstream);
                return pResult;
            }
        }
        
        // WARNING. newRecord() and newHeap() must be after _group_end detection
        // do the hard stuff
        
        this.isEmptyCursor = false;
        long pResult = populateRecord(pRecUpstream);
        this.counter.incrementAndGet();
        return pResult;
    }

    @Override
    public void close() {
        this.upstream.close();
        super.close();
    }

    @Override
    public String toString() {
        return "Aggregator";
    }

	private long populateRecord(long pRecord) {
        long pResult = newRecord();
        Heap heap = newHeap();
        for (int i=0; i<this.exprs.size(); i++) {
            Operator it = this.exprs.get(i);
            long pValue = it.eval(ctx, heap, params, pRecord);
            Record.set(pResult, i, pValue);
        };
        return pResult;
    }
    
    private void resetAggregationFunctions(long pRecord) {
        for (int i=0; i<this.exprs.size(); i++) {
            Operator it = this.exprs.get(i);
            it.eval(ctx, getHeap(), params, pRecord);
        }
    }

    public void setSource(Script source) {
        this.source = source;
    }
    
    public Script getSource() {
        return this.source;
    }
}
