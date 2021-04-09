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

public class Filter extends CursorMaker {
    CursorMaker upstream;
    Operator expr;
    boolean outer;
    
    public Filter(CursorMaker upstream, Operator expr, int makerId) {
        this(upstream, expr, false, makerId);
    }
    
    public Filter(CursorMaker upstream, Operator expr, boolean outer, int makerId) {
        super();
        this.upstream = upstream;
        this.expr = expr;
        this.outer = outer;
        setMakerId(makerId);
    }

    @Override
    public CursorMeta getCursorMeta() {
        return this.upstream.getCursorMeta();
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        Cursor c = upstream.make(ctx, params, pMaster);
        c = new FilteredCursor(ctx, params, c, this.expr, this.outer, ctx.getCursorStats(makerId));
        return c;
    }

    @Override
    public void explain(int level, List<ExplainRecord> records) {
        String text = getClass().getSimpleName() + "(" + this.expr.toString() + ")";
        ExplainRecord rec = new ExplainRecord(getMakerid(), level, text, 0);
        records.add(rec);
        this.upstream.explain(level+1, records);
        this.expr.visit(it ->{
                if (it instanceof OpInSelect) {
                    OpInSelect select = (OpInSelect)it;
                    select.explain(level+1, records);
                }
        });
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
