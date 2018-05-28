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

import com.antsdb.saltedfish.cpp.BluntHeap;
import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.RowIterator;
import com.antsdb.saltedfish.nosql.ScanOptions;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.IndexMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.sql.planner.SortKey;

/**
 * used by LIKE operator in a prepared statement. it is hard to decide the best plan at the preparation 
 * time.
 * 
 * @author *-xguo0<@
 */
public class UncertainScan extends CursorMaker implements Ordered {

    private CursorMeta meta;
    private int[] mapping;
    private TableMeta table;
    private IndexMeta index;
    private Operator likeOperand;
    private Operator like;

    public UncertainScan(TableMeta table, IndexMeta index, int nextMakerId, Operator like, Operator operand) {
        this.table = table;
        this.index = index;
        this.meta = CursorMeta.from(table);
        this.mapping = this.meta.getHumpbackMapping();
        this.like = like;
        this.likeOperand = operand;
        setMakerId(makerId);
   }

    @Override
    public CursorMeta getCursorMeta() {
        return this.meta;
    }

    @Override
    public boolean setSortingOrder(List<SortKey> order) {
        return false;
    }

    @Override
    public List<ColumnMeta> getOrder() {
        return this.index.getColumns(table);
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        try (BluntHeap heap = new BluntHeap()) {
            // get the LIKE operand 
            
            long pOperand = this.likeOperand.eval(ctx, heap, params, pMaster);
            if (pOperand == 0) {
                return new EmptyCursor(meta);
            }
            String operand = (String)FishObject.get(heap, pOperand);
            
            // depends on the value of the operand, create different cursors
            
            int idxOfPercent = operand.indexOf('%');
            int idxOfUnderscore = operand.indexOf('_');
            if ((idxOfPercent == 0) || (idxOfUnderscore == 0)) {
                // table scan if the pattern has a leading wildcard character
                return createTableScan(ctx, params, pMaster);
            }
            if ((idxOfPercent < 0) && (idxOfUnderscore < 0)) {
                // index seek if the pattern has no wildcard characters
                return createIndexSeek(ctx, heap, operand, params);
            }
            // index scan if the pattern trailing wildcard
            int idx;
            if ((idxOfPercent < 0) || (idxOfUnderscore < 0)) {
                idx = Math.max(idxOfPercent, idxOfUnderscore);
            }
            else {
                idx = Math.min(idxOfPercent, idxOfUnderscore);
            }
            return createIndexScan(ctx, heap, operand.substring(0, idx), params);
        }
    }

    private Cursor createIndexScan(VdmContext ctx, Heap heap, String prefix, Parameters params) {
        GTable gindex = ctx.getHumpback().getTable(this.index.getIndexTableId());
        GTable gtable = ctx.getHumpback().getTable(this.table.getHtableId());
        Transaction trx = ctx.getTransaction();
        long options = 0;
        long pKeyFrom = KeyMaker.make(heap, prefix);
        long pKeyTo = KeyMaker.make(heap, prefix + "\uffff");
        options = ScanOptions.excludeEnd(options);
        RowIterator it = gindex.scan(trx.getTrxId(), trx.getTrxTs(), pKeyFrom, pKeyTo, options);
        IndexCursor cursor = new IndexCursor(
                ctx.getSpaceManager(), 
                this.meta, 
                it, 
                mapping, 
                gtable, 
                trx, 
                ctx.getCursorStats(makerId));
        FilteredCursor result = new FilteredCursor(ctx, params, cursor, this.like, false, new AtomicLong());
        return result;
    }

    private Cursor createIndexSeek(VdmContext ctx, Heap heap, String prefix, Parameters params) {
        // for now, lets use index scan. it is slightly efficient 
        return createIndexScan(ctx, heap, prefix, params);
    }

    private Cursor createTableScan(VdmContext ctx, Parameters params, long pMaster) {
        CursorMaker maker = new TableScan(this.table, this.makerId);
        Cursor cursor = maker.make(ctx, params, pMaster);
        FilteredCursor result = new FilteredCursor(ctx, params, cursor, this.like, false, new AtomicLong());
        return result;
    }

}
