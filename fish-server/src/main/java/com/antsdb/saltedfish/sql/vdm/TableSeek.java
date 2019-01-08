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

import java.util.Collections;
import java.util.List;

import com.antsdb.saltedfish.cpp.BluntHeap;
import com.antsdb.saltedfish.cpp.FishBoundary;
import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.sql.planner.SortKey;
import com.antsdb.saltedfish.util.CursorUtil;

public class TableSeek extends CursorMaker implements Seekable {
    CursorMeta meta;
    int[] mapping;
    TableMeta table;
    Operator op;
    private Vector key;

    class MyCursor extends CursorWithHeap {
        long pRow;

        public MyCursor(long pRow) {
            super(TableSeek.this.meta);
            this.pRow = pRow;
        }

        @Override
        public long next() {
            if (this.pRow == 0) {
                return 0;
            }
            long pRecord = newRecord();
            Row row = Row.fromMemoryPointer(pRow, 0);
            Record.setKey(pRecord, row.getKeyAddress());
            for (int i=0; i<this.meta.getColumnCount(); i++) {
                long pValue = row.getFieldAddress(TableSeek.this.mapping[i]);
                Record.set(pRecord, i, pValue);
            }
            this.pRow = 0;
            return pRecord;
        }

        @Override
        public void close() {
            super.close();
        }

    }
    
    public TableSeek(TableMeta table, int makerId) {
        this.table = table;
        this.meta = CursorMeta.from(table);
        this.mapping = this.meta.getHumpbackMapping();
        setMakerId(makerId);
    }

    @Override
    public CursorMeta getCursorMeta() {
        return meta;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        if (this.op == null) {
            return null;
        }
        try (BluntHeap heap = new BluntHeap()) {
            long pBoundary = this.op.eval(ctx, heap, params, pMaster);
            if (pBoundary == 0) {
                Cursor c = CursorUtil.toCursor(meta, Collections.emptyList());
                return c;
            }
            FishBoundary boundary = new FishBoundary(pBoundary);
            long pKey = boundary.getKeyAddress();
            GTable gtable = ctx.getHumpback().getTable(table.getHtableId());
            Transaction trx = ctx.getTransaction();
            long pRow = gtable.get(trx.getTrxId(), trx.getTrxTs(), pKey);
            if (pRow != 0) {
                ctx.getCursorStats(makerId).incrementAndGet();
            }
            return new MyCursor(pRow);
        }
    }

    @Override
    public String toString() {
        return "Table Seek (" + this.table.getObjectName() + ")";
    }

    @Override
    public void explain(int level, List<ExplainRecord> records) {
        ExplainRecord rec = new ExplainRecord(getMakerid(), level, toString());
        records.add(rec);
    }

    @Override
    public void setSeekKey(Vector key) {
        this.key = key;
        this.op = new FuncGenerateKey(this.table.getKeyMaker(), key, false);
    }

    @Override
    public Vector getSeekKey() {
        return this.key;
    }

    public Operator getKey() {
        return this.op;
    }

    @Override
    public boolean setSortingOrder(List<SortKey> order) {
        return false;
    }

}
