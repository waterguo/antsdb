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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.RowIterator;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.planner.SortKey;

/**
 * 
 * @author *-xguo0<@
 */
public class HSelectIndex extends CursorMaker {

    protected GTable gindex;
    private CursorMeta meta;

    private class MyCursor extends CursorWithHeap {

        private RowIterator scan;

        public MyCursor(CursorMeta meta, RowIterator scan) {
            super(meta);
            this.scan = scan;
        }

        @Override
        public long next() {
            long pRecord = newRecord();
            if (scan.next()) {
                long pIndexKey = this.scan.getKeyPointer();
                long pRowKey = this.scan.getRowKeyPointer();
                Record.setKey(pRecord, pIndexKey);
                Record.set(pRecord, 0, AutoCaster.toString(getHeap(), pIndexKey));
                Record.set(pRecord, 1, AutoCaster.toString(getHeap(), pRowKey));
                Record.set(pRecord, 2, this.scan.getMisc());
                return pRecord;
            }
            else {
                return 0;
            }
        }

        @Override
        public void close() {
            this.scan.close();
        }

        @Override
        public String toString() {
            return this.scan.toString();
        }
    }
    
    public HSelectIndex(GTable gindex) {
        this.gindex = gindex;
        this.meta = new CursorMeta();
        this.meta.addColumn(new FieldMeta("00", DataType.varchar()));
        this.meta.addColumn(new FieldMeta("0", DataType.varchar()));
        this.meta.addColumn(new FieldMeta("1", DataType.integer()));
    }

    @Override
    public CursorMeta getCursorMeta() {
        return meta;
    }

    @Override
    public boolean setSortingOrder(List<SortKey> order) {
        return false;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        Transaction trx = ctx.getTransaction();
        RowIterator scan = gindex.scan(trx.getTrxId(), trx.getTrxTs(), 0, 0, 0);
        return wrap(new MyCursor(this.meta, scan), params);
    }
    
    protected ExprCursor wrap(Cursor upstream, Parameters params) {
        ExprCursor result = new ExprCursor(meta, upstream, params, new AtomicLong());
        result.exprs = new ArrayList<>();
        for (FieldMeta field:meta.getColumns()) {
            ToString op = new ToString(new FieldValue(field, result.exprs.size()));
            result.exprs.add(op);
        }
        return result;
    }
    
}
