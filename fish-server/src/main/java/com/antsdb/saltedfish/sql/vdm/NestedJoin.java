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

import com.antsdb.saltedfish.sql.planner.SortKey;

/**
 * most straight forward join by passing record one by one to the right side cursor maker. 
 * 
 * @author xguo
 * @see https://technet.microsoft.com/en-us/library/ms191318(v=sql.105).aspx
 */
public class NestedJoin extends CursorMaker {
    CursorMaker left;
    CursorMaker right;
    CursorMeta meta;
    boolean isOuter;
    Operator condition;
    
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
            super(NestedJoin.this.meta);
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
                    Boolean b = (Boolean)Util.eval(ctx, NestedJoin.this.condition, this.params, pRecord);
                    if ( (b == null) || (!b)) {
                        continue;
                    }
                }
                return pRecord;
            }
        }
        
        private void fetchNextLeft() {
            this.pRecLeft = this.left.next();
            if (this.right != null) {
                this.right.close();
                this.right = null;
            }
            if (this.pRecLeft != 0) {
                Record.size(this.pRecLeft);
                this.counter.incrementAndGet();
                this.right = makeRightCursor(this.pRecLeft);
                this.hasLeftMatched = false;
            }
        }

        private Cursor makeRightCursor(long rec) {
            Cursor c = NestedJoin.this.right.make(ctx, params, rec);
            return c;
        }

        @Override
        public void close() {
            super.close();
            this.left.close();
            if (this.right != null) {
                this.right.close();
            }
        }

        @Override
        public String toString() {
            return "Nested Join";
        }

        long makeJoinedRecord(long pRecordLeft, long pRecordRight) {
            long pResult = newRecord();
            int nFieldsLeft = Record.size(pRecordLeft);
            for (int i=0; i<nFieldsLeft; i++) {
                long pValue = Record.get(pRecordLeft, i);
                Record.set(pResult, i, pValue);
            }
            if (pRecordRight != 0) {
                for (int i=0; i<Record.size(pRecordRight); i++) {
                    long pValue = Record.get(pRecordRight, i);
                    Record.set(pResult, nFieldsLeft + i, pValue);
                }
            }
            return pResult;
        }
    }
    
    public NestedJoin(CursorMaker left, CursorMaker right, Operator condition, boolean isOuter, int makerId) {
        super();
        this.meta = CursorMeta.join(left.getCursorMeta(), right.getCursorMeta());
        this.left = left;
        this.right = right;
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
        ExplainRecord rec = new ExplainRecord(getMakerid(), level, toString());
        records.add(rec);
        this.left.explain(level+1, records);
        this.right.explain(level+1, records);
    }

    @Override
    public boolean setSortingOrder(List<SortKey> order) {
        return this.left.setSortingOrder(order);
    }

}
