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

import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.FlexibleHeap;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.HumpbackError;
import com.antsdb.saltedfish.nosql.VaporizingRow;
import com.antsdb.saltedfish.sql.LockLevel;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class InsertSelect extends Statement {
    static Logger _log = UberUtil.getThisLogger();

    private CursorMaker maker;
    private TableMeta table;
    private boolean isReplace;
    private boolean ignoreError;
    private int tableId;
    private GTable gtable;
    private IndexEntryHandlers indexHandlers;

    public InsertSelect(Orca orca,
                        GTable gtable, 
                        TableMeta table, 
                        CursorMaker maker, 
                        boolean isReplace, 
                        boolean ignoreError) {
        this.maker = maker;
        this.table = table;
        this.isReplace = isReplace;
        this.ignoreError = ignoreError;
        this.tableId = table.getId();
        this.gtable = gtable;
        this.indexHandlers = new IndexEntryHandlers(orca, table);
    }

    @Override
    public Object run(VdmContext ctx, Parameters params) {
        ctx.getSession().lockTable(this.tableId, LockLevel.SHARED, true);
        try (Cursor c = this.maker.make(ctx, params, 0)) {
            try (Heap heap = new FlexibleHeap()) {
                int count = 0;
                for (;;) {
                    long pRecord = c.next();
                    if (pRecord == 0) {
                        break;
                    }
                    if (this.ignoreError) {
                        try {
                            insertRow(ctx, heap, pRecord);
                            count++;
                        }
                        catch (Exception x) {
                            _log.debug("error from insert is ignored", x);
                            return false;
                        }
                    }
                    else {
                        insertRow(ctx, heap, pRecord);
                        count++;
                    }
                }
                return count;
            }
        }
    }

    VaporizingRow genRow(VdmContext ctx, Heap heap, long pRecord) {
        VaporizingRow row = new VaporizingRow(heap, this.table.getMaxColumnId());
        row.set(0, ctx.getOrca().getNextRowid());
        List<ColumnMeta> fields = this.table.getColumns();
        for (int i=0; i<fields.size(); i++) {
            long pValue = Record.getValueAddress(pRecord, i);
            ColumnMeta column = fields.get(i);
            row.setFieldAddress(column.getColumnId(), pValue);
        }
        long pKey = this.table.getKeyMaker().make(heap, row);
        row.setKey(pKey);
        return row;
    }

    void insertRow(VdmContext ctx, Heap heap, long pRecord) {
        int timeout = ctx.getSession().getConfig().getLockTimeout();
        VaporizingRow row = genRow(ctx, heap, pRecord);
        
        // do it
        
        Transaction trx = ctx.getTransaction();
        row.setVersion(trx.getGuaranteedTrxId());
        for (;;) {
            HumpbackError error = isReplace ? this.gtable.put(row, timeout) : this.gtable.insert(row, timeout);
            if (error == HumpbackError.SUCCESS) {
                this.indexHandlers.insert(heap, trx, row, timeout, isReplace);
                break;
            }
            else {
                throw new OrcaException(error);
            }
        }
    }
    
    @Override
    List<TableMeta> getDependents() {
        List<TableMeta> list = new ArrayList<TableMeta>();
        list.add(this.table); 
        return list;
    }

}
