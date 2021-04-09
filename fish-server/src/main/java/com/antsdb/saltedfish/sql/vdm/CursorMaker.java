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

import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.planner.SortKey;

public abstract class CursorMaker extends Instruction {
    int makerId;
    
    public abstract CursorMeta getCursorMeta();
    public abstract boolean setSortingOrder(List<SortKey> order);

    /**
     * give the maker last chance to dosomething
     * @param c
     */
    public void demolish(Cursor c) {
    }
    
    /**
     * 
     * @param ctx
     * @param params
     * @param pMaster
     * @param last nullable, last cursor returned from the same maker for recycling purpose
     * @return
     */
    public Cursor make(VdmContext ctx, Parameters params, long pMaster, Cursor last) {
        return (Cursor)run(ctx, params, pMaster);
    }
    
    public Cursor make(VdmContext ctx, Parameters params, long pMaster) {
        return make(ctx, params, pMaster, null);
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        return null;
    }

    @Override
    public void explain(int level, List<ExplainRecord> records) {
        ExplainRecord rec = new ExplainRecord(getMakerid(),  level, toString(), getScore());
        if (this instanceof Ordered) {
            List<ColumnMeta> order = ((Ordered)this).getOrder();
            if (order != null && !order.isEmpty()) {
                StringBuilder buf = new StringBuilder();
                for (ColumnMeta i:order) {
                    buf.append(i.getColumnName());
                    buf.append(',');
                }
                buf.deleteCharAt(buf.length()-1);
                rec.order = buf.toString();
            }
        }
        records.add(rec);
    }
    
    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    public int getMakerid() {
        return this.makerId;
    }

    public void setMakerId(int value) {
        this.makerId = value;
    }
    
    /**
     * the smaller the faster, the bigger the slower. a single get is 1, a index seek is 1.1, a table scan is 100
     * @return
     */
    public abstract float getScore();
    
    /**
     * return the order of rows from the cursor
     * 
     * @return nullable, array of position of the record fields from the cursor
     */
}
