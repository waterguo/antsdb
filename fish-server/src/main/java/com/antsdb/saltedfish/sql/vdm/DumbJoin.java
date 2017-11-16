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

import java.util.List;

import com.antsdb.saltedfish.sql.planner.SortKey;

/**
 * 
 * also known as cross join
 * 
 * @author xinyi
 *
 */
public class DumbJoin extends CursorMaker {
    CursorMeta meta = new CursorMeta();
    CursorMaker left;
    CursorMaker right;
    Operator expr;
    boolean outer;

    static class JoinedCursor extends CursorWithHeap {

        public boolean isOuterJoin;
        public Parameters params;
        public VdmContext ctx;
        public Operator expr;
        public Cursor left;
        Cursor right;
        CursorMaker makerRight;
        int pointer = -1;
        long pRecLeft;
        boolean recLeftMatched = false;

        public JoinedCursor(VdmContext ctx, Parameters params, CursorMeta meta, Cursor left, CursorMaker right) {
            super(meta);
            this.ctx = ctx;
            this.params = params;
            this.left = left;
            this.makerRight = right;
            fetchNextLeft();
        }

        @Override
        public long next() {
            for (;;) {
                // is eof
                
                if (this.pRecLeft == 0) {
                    return 0;
                }
                
                // step on the righth side cursor
                
                long pRec = next_();
                if (pRec != 0) {
                    this.recLeftMatched = true;
                    return pRec;
                }
                
                // running to eof on the right side cursor
                
                if (this.isOuterJoin && !recLeftMatched) {
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
                long pResult = makeJoinedRecord(this.pRecLeft, pRecRight);
                if (checkRecord(pResult)) {
                    return pResult;
                }
            }
        }
        
        private void fetchNextLeft() {
            this.pRecLeft = this.left.next();
            this.recLeftMatched = false;
            if (this.right != null) {
                this.right.close();
                this.right = null;
            }
            this.right = this.makerRight.make(this.ctx, this.params, this.pRecLeft);
        }

        @Override
        public void close() {
        	super.close();
            this.left.close();
            if (this.right != null) {
                this.right.close();
            }
        }
        
        boolean checkRecord(long pRec) {
            if (this.expr == null) {
                return true;
            }
            Object result = Util.eval(ctx, expr, this.params, pRec);
            if (!(result instanceof Boolean)) {
                throw new IllegalArgumentException();
            }
            return ((Boolean)result);
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
    
    public DumbJoin(CursorMaker left, CursorMaker right, CursorMeta meta, Operator condition, boolean outer) {
        this.left = left;
        this.right = right;
        this.outer = outer;
        this.expr = condition;
        this.meta = meta;
    }

    @Override
    public CursorMeta getCursorMeta() {
        return this.meta;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        Cursor left = this.left.make(ctx, params, pMaster);
        JoinedCursor c = new JoinedCursor(ctx, params, getCursorMeta(), left, this.right);
        c.expr = this.expr;
        c.isOuterJoin = this.outer;
        return c;
    }

    @Override
    public boolean setSortingOrder(List<SortKey> order) {
        return this.left.setSortingOrder(order);
    }

}
