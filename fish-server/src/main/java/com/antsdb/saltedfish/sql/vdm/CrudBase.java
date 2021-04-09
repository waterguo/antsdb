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

import com.antsdb.saltedfish.cpp.BluntHeap;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.HumpbackError;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.meta.Constraint;
import com.antsdb.saltedfish.sql.meta.TableMeta;

/**
 * 
 * @author *-xguo0<@
 */
abstract class CrudBase extends Statement {
    GTable gtable;
    IndexEntryHandlers indexHandlers;
    TableMeta table;
    GTable blobTable;
    List<Constraint> constraints;

    CrudBase(Orca orca, TableMeta table, GTable gtable) {
        super();
        this.table = table;
        this.gtable = gtable;
        this.indexHandlers = new IndexEntryHandlers(orca, table);
        this.blobTable = orca.getHumpback().getTable(table.getBlobTableId());
    }
    
    protected boolean deleteSingleRow(VdmContext ctx, Row row) {
        Transaction trx = ctx.getTransaction();
        int timeout = ctx.getSession().getConfig().getLockTimeout();
        try (Heap heap = new BluntHeap()) {
            heap.reset(0);
            long trxid = trx.getGuaranteedTrxId();
            long error = this.gtable.deleteRow(ctx.getHSession(), trxid, row.getAddress(), timeout);
            if (HumpbackError.isSuccess(error)) {
                if (this.blobTable != null) {
                    error = this.blobTable.deleteRow(ctx.getHSession(), trxid, row.getAddress(), timeout);
                    if (!HumpbackError.isSuccess(error) && (error != HumpbackError.MISSING)) {
                        throw new OrcaException(HumpbackError.toString(error));
                    }
                }
                this.indexHandlers.delete(heap, ctx.getHSession(), trx, row, timeout);
                return true;
            }
            else if (error == HumpbackError.MISSING) {
                // row got deleted in a concurrency session
                return false;
            }
            else {
                throw new OrcaException(HumpbackError.toString(error));
            }
        }
    }
    
    public void prepareConstraints(GeneratorContext ctx) {
//        for (ForeignKeyMeta i:table.getForeignKeys()) {
//            if (this.constraints == null) {
//                this.constraints = new ArrayList<>();
//            }
//            ForeignKeyConstraint ii = new ForeignKeyConstraint(this.table, i);
//            ii.prepare(ctx);
//            this.constraints.add(ii);
//        }
    }
}
