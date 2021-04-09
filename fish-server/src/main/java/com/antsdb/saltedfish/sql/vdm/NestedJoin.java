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

import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.FishBool;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.Value;
import com.antsdb.saltedfish.sql.planner.SortKey;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * most straight forward join by passing record one by one to the right side cursor maker. 
 * 
 * @author xguo
 * @see https://technet.microsoft.com/en-us/library/ms191318(v=sql.105).aspx
 */
public class NestedJoin extends CursorMaker {
    final static Logger _log = UberUtil.getThisLogger();
    
    public CursorMaker left;
    public CursorMaker right;
    CursorMeta meta;
    boolean isOuter;
    Operator condition;
    private int widthx;
    private int widthy;
    
    class JoinedCursor extends CursorWithHeap {

        Parameters params;
        VdmContext ctx;
        Cursor left;
        Cursor right;
        long pRecLeft = -1;
        boolean hasLeftMatched = false;
        private AtomicLong counter;
        long pRecord;

        public JoinedCursor(VdmContext ctx, Parameters params, Cursor left, AtomicLong counter) {
            super(NestedJoin.this.meta, NestedJoin.this.widthx + NestedJoin.this.widthy);
            this.ctx = ctx;
            this.params = params;
            this.left = left;
            this.counter = counter;
        }

        @Override
        public long next() {
            if (this.pRecLeft == -1) {
                fetchNextLeft();
            }
            for (;;) {
                // is eof
                
                if (this.pRecLeft == 0) {
                    return 0;
                }
                
                // step on the right side cursor
                
                long pRec = next_();
                if (pRec != 0) {
                    this.hasLeftMatched = true;
                    return pRec;
                }
                
                // running to eof on the right side cursor
                
                if (NestedJoin.this.isOuter && !this.hasLeftMatched) {
                    // for outer join item without nothing matched on the right side, return record on the left
                    pRec = makeJoinedRecord(this.pRecLeft, 0);
                    fetchNextLeft();
                    return pRec;
                }
                else {
                    fetchNextLeft();
                }
            }
        }
        
        long next_() {
            for (;;) {
                long pRecRight = this.right.next();
                if (pRecRight == 0) {
                    return 0;
                }
                this.counter.incrementAndGet();
                long pRecord = makeJoinedRecord(this.pRecLeft, pRecRight);
                if (NestedJoin.this.condition != null) {
                    Heap heap = getHeap();
                    long pBool = NestedJoin.this.condition.eval(ctx, heap, params, pRecord);
                    if (pBool == 0) continue;
                    boolean b = FishBool.get(heap, pBool);
                    if (!b) continue;
                }
                return pRecord;
            }
        }
        
        private void fetchNextLeft() {
            this.pRecLeft = this.left.next();
            if (this.right != null) {
                this.right.close();
            }
            if (this.pRecLeft != 0) {
                this.counter.incrementAndGet();
                makeRightCursor(this.pRecLeft);
                this.hasLeftMatched = false;
            }
        }

        private void makeRightCursor(long rec) {
            this.right = NestedJoin.this.right.make(ctx, params, rec, this.right);
        }

        @Override
        public void close() {
            super.close();
            this.left.close();
            if (this.right != null) {
                this.right.close();
            }
            NestedJoin.this.right.demolish(this.right);
        }

        @Override
        public String toString() {
            return "Nested Join";
        }

        long makeJoinedRecord(long pRecordLeft, long pRecordRight) {
            check(pRecordLeft);
            check(pRecordRight);
            long pResult = newRecord();
            for (int i=0; i<NestedJoin.this.widthx; i++) {
                long pValue = Record.get(pRecordLeft, i);
                Record.set(pResult, i, pValue);
            }
            if (pRecordRight != 0) {
                for (int i=0; i<NestedJoin.this.widthy; i++) {
                    long pValue = Record.get(pRecordRight, i);
                    Record.set(pResult, NestedJoin.this.widthx + i, pValue);
                }
            }
            return pResult;
        }
        
        private void check(long pRecord) {
            if (pRecord == 0) {
                return;
            }
            byte format = Value.getFormat(null, pRecord);
            if (format == Value.FORMAT_RECORD) {
                return;
            }
            _log.warn("check failed format={} count={}", format, this.counter.get());
        }
    }
    
    public NestedJoin(
            CursorMaker x, 
            CursorMaker y, 
            int widthx, 
            int widthy, 
            Operator condition, 
            boolean isOuter, 
            int makerId) {
        super();
        this.meta = CursorMeta.join(x.getCursorMeta(), y.getCursorMeta());
        this.left = x;
        this.right = y;
        this.widthx = widthx;
        this.widthy = widthy;
        this.condition = condition;
        this.isOuter = isOuter;
        setMakerId(makerId);
    }

    @Override
    public CursorMeta getCursorMeta() {
        return meta;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        Cursor left = this.left.make(ctx, params, pMaster);
        JoinedCursor c = new JoinedCursor(ctx, params, left, ctx.getCursorStats(makerId));
        return c;
    }

    @Override
    public String toString() {
        String join = (this.condition != null) ? this.condition.toString() : "";
        return String.format("Nested Join (%d - %d) ON (%s)",
                             this.left.makerId,
                             this.right.makerId,
                             join);
    }

    @Override
    public void explain(int level, List<ExplainRecord> records) {
        super.explain(level, records);
        this.left.explain(level+1, records);
        this.right.explain(level+1, records);
    }

    @Override
    public boolean setSortingOrder(List<SortKey> order) {
        return this.left.setSortingOrder(order);
    }

    @Override
    public float getScore() {
        return this.left.getScore() * this.right.getScore();
    }

    public CursorMaker getLeft() { 
        return this.left;
    }
}
