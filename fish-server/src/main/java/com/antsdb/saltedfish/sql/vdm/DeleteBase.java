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

import com.antsdb.saltedfish.cpp.BluntHeap;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.HumpbackError;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.meta.TableMeta;

/**
 * common logic for DETELE statement
 * @author wgu0
 */
abstract class DeleteBase extends Statement {
    GTable gtable;
    IndexEntryHandlers indexHandlers;
    TableMeta table;
    private GTable blobTable;
    
    public DeleteBase(Orca orca, TableMeta table, GTable gtable) {
        super();
        this.table = table;
        this.gtable = gtable;
        this.indexHandlers = new IndexEntryHandlers(orca, table);
        this.blobTable = orca.getHumpback().getTable(table.getBlobTableId());
    }

    protected boolean deleteSingleRow(VdmContext ctx, Parameters params, long pKey) {
            Transaction trx = ctx.getTransaction();
            int timeout = ctx.getSession().getConfig().getLockTimeout();
        try (Heap heap = new BluntHeap()) {
            heap.reset(0);
            Row row = null;
            row = this.gtable.getRow(trx.getTrxId(), trx.getTrxTs(), pKey);
            HumpbackError error = this.gtable.deleteRow(trx.getGuaranteedTrxId(), row.getAddress(), timeout);
            if (error == HumpbackError.SUCCESS) {
                if (this.blobTable != null) {
                    error = this.blobTable.deleteRow(trx.getGuaranteedTrxId(), row.getAddress(), timeout);
                    if ((error != HumpbackError.SUCCESS) && (error != HumpbackError.MISSING)) {
                        throw new OrcaException(error);
                    }
                }
                this.indexHandlers.delete(heap, trx, row, timeout);
                return true;
            }
            else if (error == HumpbackError.MISSING) {
                // row got deleted in a concurrency session
                return false;
            }
            else {
                throw new OrcaException(error);
            }
        }
    }

}
