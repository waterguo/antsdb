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
import com.antsdb.saltedfish.util.LatencyDetector;
import com.antsdb.saltedfish.util.UberUtil;

public class InsertSingleRow extends Statement {
    static Logger _log = UberUtil.getThisLogger();
    
    TableMeta table;
    GTable gtable;
    Operator[] values;
    IndexEntryHandlers indexHandlers;
    boolean ignoreError = false;
    int tableId;
    boolean isReplace = false;
    boolean hasBlob = false;
    GTable blobTable = null;
    
    public InsertSingleRow(Orca orca, TableMeta table, GTable gtable, Operator[] values) {
        this.table = table;
        this.gtable = gtable;
        this.values = values;
        this.indexHandlers = new IndexEntryHandlers(orca, table);
        this.tableId = table.getId();
        for (ColumnMeta i:table.getColumns()) {
            if (i.isBlobClob()) {
                this.blobTable = orca.getHumpback().getTable(table.getBlobTableId());
                this.hasBlob = true;
                break;
            }
        }
    }

    @Override
    public Object run(VdmContext ctx, Parameters params) {
        try {
            LatencyDetector.run(_log, "lockTable", ()->{
                ctx.getSession().lockTable(this.tableId, LockLevel.SHARED, true);
                return null;
            });
            return run_(ctx, params);
        }
        finally {
        }
    }

    public void setReplace(boolean value) {
        this.isReplace = value;
    }
    
    public Object run_(VdmContext ctx, Parameters params) {
        int count = 0;
        try (Heap heap = new FlexibleHeap()) {
            heap.reset(0);
            if (this.ignoreError) {
                try {
                    insertRow(ctx, heap, params, 0);
                    count++;
                    return count;
                }
                catch (Exception x) {
                    _log.debug("error from insert is ignored", x);
                    return false;
                }
            }
            else {
                insertRow(ctx, heap, params, 0);
                count++;
            }
        }
        return count;
    }

    VaporizingRow genRow(VdmContext ctx, Heap heap, Parameters params, VaporizingRow blob, long pRecord) {
        // collect values 
        
        VaporizingRow row = new VaporizingRow(heap, this.table.getMaxColumnId());
        row.set(0, ctx.getOrca().getNextRowid());
        for (int i=0; i<this.values.length; i++) {
            Operator expr = values[i];
            if (expr != null) {
                long pValue = expr.eval(ctx, heap, params, pRecord);
                row.setFieldAddress(i, pValue);
            }
        }
        
        // collect key
        
        long pKey = this.table.getKeyMaker().make(heap, row);
        row.setKey(pKey);
        
        // blob fields

        if (blob != null) {
            blob.setKey(pKey);
            for (ColumnMeta column:this.table.getColumns()) {
                if (column.isBlobClob()) {
                    long pValue = row.getFieldAddress(column.getColumnId());
                    if (pValue != 0) {
                        BlobReference blobref = BlobReference.alloc(heap, pKey, pValue);
                        row.setFieldAddress(column.getColumnId(), blobref.addr);
                        blob.setFieldAddress(column.getColumnId(), pValue);
                    }
                }
            }
        }
        
        return row;
    }
    
    void insertRow(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        int timeout = ctx.getSession().getConfig().getLockTimeout();
        Transaction trx = ctx.getTransaction();
        
        // collect values
        
        VaporizingRow rowBlob = null;
        if (this.hasBlob) {
            int maxBlobColumnId = this.table.getMaxBlobColumnId();
            rowBlob = new VaporizingRow(heap, maxBlobColumnId);
        }
        VaporizingRow row = genRow(ctx, heap, params, rowBlob, pRecord);
        
        // collect key
        
        long pKey = this.table.getKeyMaker().make(heap, row);
        row.setKey(pKey);
        
        // do it
        
        row.setVersion(trx.getGuaranteedTrxId());
        if (this.hasBlob) {
            rowBlob.setKey(row.getKeyAddress());
            rowBlob.setVersion(row.getVersion());
        }
        for (;;) {
            HumpbackError error = isReplace ? 
                                  this.gtable.put(ctx.getHSession(), row, timeout) : 
                                  this.gtable.insert(ctx.getHSession(), row, timeout);
            if (error == HumpbackError.SUCCESS) {
                if (this.hasBlob) {
                    error = this.blobTable.insert(ctx.getHSession(), rowBlob, timeout);
                    if (error != HumpbackError.SUCCESS) {
                        throw new OrcaException(error, row.getKeySpec(this.tableId));
                    }
                }
                this.indexHandlers.insert(heap, ctx.getHSession(), trx, row, timeout, isReplace);
                break;
            }
            else {
                throw new OrcaException(error, row.getKeySpec(this.tableId));
            }
        }
    }
    
    @Override
    List<TableMeta> getDependents() {
        List<TableMeta> list = new ArrayList<TableMeta>();
        list.add(this.table);
        return list;
    }

    public void setIgnoreError(boolean b) {
        this.ignoreError = b;
    }

}
