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

import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.HumpbackError;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.VaporizingRow;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.PrimaryKeyMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * common logic for UPDATE statement
 *  
 * @author wgu0
 */
abstract class UpdateBase extends Statement {
    static Logger _log = UberUtil.getThisLogger();
    
    GTable gtable;
    List<ColumnMeta> columns;
    List<Operator> values;
    IndexEntryHandlers indexHandlers;
    TableMeta table;
    boolean isPrimaryKeyAffected = false;
    private GTable blobTable;
    private boolean hasBlob;

    public UpdateBase(Orca orca, TableMeta table, GTable gtable, List<ColumnMeta> columns, List<Operator> values) {
        super();
        this.gtable = gtable;
        this.columns = columns;
        this.values = values;
        this.table = table;
        this.indexHandlers = new IndexEntryHandlers(orca, table);
        PrimaryKeyMeta pk = table.getPrimaryKey();
        if (pk != null) {
            List<ColumnMeta> pkColumns = table.getPrimaryKey().getColumns(table);
            for (ColumnMeta i:columns) {
                if (pkColumns.contains(i)) {
                    this.isPrimaryKeyAffected = true;
                    break;
                }
            }
        }
        for (ColumnMeta i:table.getColumns()) {
            if (i.isBlobClob()) {
                this.blobTable = orca.getHumpback().getTable(table.getBlobTableId());
                this.hasBlob = true;
                break;
            }
        }
    }

    protected boolean updateSingleRow(VdmContext ctx, Heap heap, Parameters params, long pKey) {
            boolean success = false;
            Transaction trx = ctx.getTransaction();
            trx.getGuaranteedTrxId();
            int timeout = ctx.getSession().getConfig().getLockTimeout();
            long heapMark = heap.position();
        for (;;) {
            heap.reset(heapMark);
            // get the __latest__ version of the row 

            Row oldRow = this.gtable.getRow(trx.getTrxId(), Long.MAX_VALUE, pKey);
            if (oldRow == null) {
                // row could be deleted between query and here
                return false;
            }
            long pRecord = Record.fromRow(heap, table, oldRow);
            VaporizingRow newRow = VaporizingRow.from(heap, this.table.getMaxColumnId(), oldRow);
            
            // update new values
            
            VaporizingRow blobRow = null;
            if (this.hasBlob) {
                Row oldBlobRow = this.blobTable.getRow(trx.getTrxId(), trx.getTrxTs(), oldRow.getKey());
                if (oldBlobRow != null) {
                    blobRow = VaporizingRow.from(heap, this.table.getMaxColumnId(), oldBlobRow);
                }
                else {
                    blobRow = new VaporizingRow(heap, this.table.getMaxColumnId());
                }
            }
            for (int i=0; i<this.columns.size(); i++) {
                ColumnMeta column = this.columns.get(i);
                Operator expr = this.values.get(i);
                long pValue = expr.eval(ctx, heap, params, pRecord);
                if (!column.isBlobClob()) {
                    newRow.setFieldAddress(column.getColumnId(), pValue);
                }
                else {
                    if (pValue != 0) {
                        BlobReference blobref = BlobReference.alloc(heap, pKey, pValue);
                        newRow.setFieldAddress(column.getColumnId(), blobref.addr);
                        blobRow.setFieldAddress(column.getColumnId(), pValue);
                    }
                    else {
                        newRow.setFieldAddress(column.getColumnId(), 0);
                        blobRow.setFieldAddress(column.getColumnId(), 0);
                    }
                }
            }
            
            // update storage

            HumpbackError error;
            boolean primaryKeyChange = false;
            if (this.isPrimaryKeyAffected) {
                long pNewKey = this.table.getKeyMaker().make(heap, newRow);
                primaryKeyChange = !KeyMaker.equals(pKey, pNewKey);
                newRow.setKey(pNewKey);
            }
            if (primaryKeyChange) {
                error = this.gtable.deleteRow(ctx.getHSession(), trx.getTrxId(), oldRow.getAddress(), timeout);
                if (error != HumpbackError.SUCCESS) {
                    throw new OrcaException(error);
                }
                newRow.setVersion(trx.getTrxId());
                error = this.gtable.insert(ctx.getHSession(), newRow, timeout);
            }
            else {
                newRow.setVersion(trx.getTrxId());
                error = this.gtable.update(ctx.getHSession(), newRow, oldRow.getTrxTimestamp(), timeout);
            }
            if (error == HumpbackError.SUCCESS) {
                if (blobRow != null) {
                    blobRow.setKey(newRow.getKeyAddress());
                    blobRow.setVersion(newRow.getVersion());
                }
                // update blob
                if (blobRow != null) {
                    if (primaryKeyChange) {
                        long pOldBlobRow = this.blobTable.get(trx.getTrxId(), trx.getTrxTs(), oldRow.getKeyAddress());
                        if (pOldBlobRow != 0) {
                            error = this.blobTable.deleteRow(ctx.getHSession(), trx.getTrxId(), pOldBlobRow, timeout);
                            if (error != HumpbackError.SUCCESS) {
                                throw new OrcaException(error);
                            }
                        }
                        error = this.blobTable.insert(ctx.getHSession(), blobRow, timeout);
                        if (error != HumpbackError.SUCCESS) {
                            throw new OrcaException(error);
                        }
                    }
                    else {
                        error = this.blobTable.put(ctx.getHSession(), blobRow, timeout);
                        if (error != HumpbackError.SUCCESS) {
                            throw new OrcaException(error);
                        }
                    }
                }
                // update indexes
                this.indexHandlers.update(heap, ctx.getHSession(), trx, oldRow, newRow, primaryKeyChange, timeout);
                success = true;
                break;
            }
            else if (error == HumpbackError.MISSING) {
                // row got deleted in a concurrency session
                success = false;
                break;
            }
            else if (error == HumpbackError.CONCURRENT_UPDATE) {
                // row is updated by another trx. retry
                continue;
            }
            else {
                throw new OrcaException(error);
            }
        }
        return success;
    }
}
