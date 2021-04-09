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

import com.antsdb.saltedfish.sql.planner.SortKey;

/**
 * group by every row
 *  
 * @author *-xguo0<@
 */
public class PerRowGrouper extends CursorMaker {
    CursorMaker upstream;

    static class MyCursor extends Cursor {
        Cursor upstream;
        boolean isEof = false;
        boolean pendingGroup = false;

        public MyCursor(Cursor upstream) {
            super(upstream.meta);
            this.upstream = upstream;
        }

        @Override
        public long next() {
            if (this.pendingGroup) {
                this.pendingGroup = false;
                return 0;
            }
            long pRecord = this.upstream.next();
            if (pRecord != 0) {
                this.pendingGroup = true;
                return pRecord;
            }
            return 0;
        }

        @Override
        public void close() {
            this.upstream.close();
        }

    }

    public PerRowGrouper(CursorMaker upstream) {
        super();
        this.upstream = upstream;
    }

    @Override
    public CursorMeta getCursorMeta() {
        return this.upstream.getCursorMeta();
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        Cursor c = this.upstream.make(ctx, params, pMaster);
        c = new MyCursor(c);
        return c;
    }

    @Override
    public void explain(int level, List<ExplainRecord> records) {
        ExplainRecord rec = new ExplainRecord(getMakerid(), level, getClass().getSimpleName(), getScore());
        records.add(rec);
        this.upstream.explain(level+1, records);
    }

    public CursorMaker getUpstream() {
        return this.upstream;
    }

    @Override
    public boolean setSortingOrder(List<SortKey> order) {
        return this.upstream.setSortingOrder(order);
    }

    @Override
    public float getScore() {
        return this.upstream.getScore();
    }
}
