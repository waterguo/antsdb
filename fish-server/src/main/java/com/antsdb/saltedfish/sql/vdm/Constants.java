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
 * 
 * @author wgu0
 */
public class Constants extends CursorMaker {
    CursorMeta meta = new CursorMeta();
    List<Operator> values;
    
    class MyCursor extends CursorWithHeap {
        int pos = 0;
        VdmContext ctx;
        Parameters params;
        long pMaster;
        
        public MyCursor(VdmContext ctx, Parameters params, long pMaster) {
            super(Constants.this.getCursorMeta());
            this.ctx = ctx;
            this.params = params;
            this.pMaster = pMaster;
        }

        @Override
        public long next() {
            if (this.pos >= Constants.this.values.size()) {
                return 0;
            }
            long pRecord = newRecord();
            long pValue = Constants.this.values.get(this.pos).eval(ctx, getHeap(), params, pRecord);
            Record.set(pRecord, 0, pValue);
            this.pos++;
            return pRecord;
        }

        @Override
        public void close() {
            super.close();
        }
    }
    
    public Constants(List<Operator> values, String name) {
        FieldMeta field = new FieldMeta(name, values.get(0).getReturnType());
        this.meta.addColumn(field);
        this.values = values;
    }
    
    @Override
    public CursorMeta getCursorMeta() {
        return this.meta;
    }

    @Override
    public Cursor make(VdmContext ctx, Parameters params, long pMaster) {
        MyCursor c = new MyCursor(ctx, params, pMaster);
        return c;
    }

    @Override
    public boolean setSortingOrder(List<SortKey> order) {
        return false;
    }
    
    @Override
    public float getScore() {
        return 0;
    }
}
