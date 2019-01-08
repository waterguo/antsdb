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

public class Aggregator extends CursorMaker {
    CursorMaker upstream;
    CursorMeta meta;
    List<Operator> exprs;
    
    public Aggregator(CursorMaker upstream, CursorMeta meta, List<Operator> exprs, int makerId) {
        super();
        this.upstream = upstream;
        this.meta = meta;
        this.exprs = exprs;
        setMakerId(makerId);
    }

    @Override
    public CursorMeta getCursorMeta() {
        return this.meta;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        ExprCursor cursor = new ExprCursor(
                this.getCursorMeta(), 
                (Cursor)this.upstream.run(ctx, params, pMaster), 
                params, 
                ctx.getCursorStats(makerId));
        cursor.ctx = ctx.freeze();
        cursor.exprs = this.exprs;
        return cursor;
    }

    @Override
    public void explain(int level, List<ExplainRecord> records) {
        ExplainRecord rec = new ExplainRecord(getMakerid(), level, toString());
        records.add(rec);
        this.upstream.explain(level+1, records);
    }

    @Override
    public String toString() {
        return "Aggregator";
    }

    public CursorMaker getUpstream() {
        return this.upstream;
    }

    @Override
    public boolean setSortingOrder(List<SortKey> order) {
        return this.upstream.setSortingOrder(order);
    }
}
