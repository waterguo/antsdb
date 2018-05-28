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

import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.planner.SortKey;

public class Union extends CursorMaker {
    CursorMaker left;
    CursorMaker right;
    boolean isUnionAll;
    CursorMeta meta;
    
    static class UnifiedCursor extends Cursor {
        Cursor left;
        Cursor right;
        private AtomicLong stats;
        
        public UnifiedCursor(Cursor left, Cursor right, AtomicLong stats) {
            super(left.meta);
            this.left = left;
            this.right = right;
            this.stats = stats;
        }

        @Override
        public long next() {
            long pRecord = this.left.next();
            if (pRecord != 0) {
                return pRecord;
            }
            this.stats.incrementAndGet();
            pRecord = this.right.next();
            return pRecord;
        }

        @Override
        public void close() {
            this.left.close();
            this.right.close();
        }
    }
    
    public Union(CursorMaker left, CursorMaker right, boolean isUnionAll, int makerId) {
        super();
        this.left = left;
        this.right = right;
        this.isUnionAll = isUnionAll;
        setMakerId(makerId);
        if (left.getCursorMeta().getColumnCount() != right.getCursorMeta().getColumnCount()) {
            throw new OrcaException("The used SELECT statement have a different number of columns");
        }
        this.meta = new CursorMeta();
        for (int i=0; i<this.left.getCursorMeta().getColumnCount(); i++) {
            FieldMeta x = this.left.getCursorMeta().getColumn(i);
            FieldMeta y = this.right.getCursorMeta().getColumn(i);
            if (x.getType().getFishType() > y.getType().getFishType()) {
                this.meta.addColumn(x);
            }
            else {
                this.meta.addColumn(y);
            }
        }
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        Cursor cLeft = this.left.make(ctx, params, pMaster);
        Cursor cRight = this.right.make(ctx, params, pMaster);
        Cursor c = new UnifiedCursor(cLeft, cRight, ctx.getCursorStats(makerId));
        if (!this.isUnionAll) {
            c = new DumbDistinctFilter.MyCursor(c);
        }
        return c;
    }

    @Override
    public CursorMeta getCursorMeta() {
        return this.meta;
    }

    @Override
    public void explain(int level, List<ExplainRecord> records) {
        super.explain(level, records);
        this.left.explain(level + 1, records);
        this.right.explain(level + 1, records);
    }

    public CursorMaker getLeft() {
        return this.left;
    }

    public CursorMaker getRight() {
        return this.right;
    }

    @Override
    public boolean setSortingOrder(List<SortKey> order) {
        return false;
    }
    
    @Override
    public String toString() {
        return String.format("UNION (%d - %d)", this.left.makerId, this.right.makerId);
    }

}
