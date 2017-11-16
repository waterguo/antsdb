/*-------------------------------------------------------------------------------------------------
 _______ __   _ _______ _______ ______  ______
 |_____| | \  |    |    |______ |     \ |_____]
 |     | |  \_|    |    ______| |_____/ |_____]

 Copyright (c) 2016, antsdb.com and/or its affiliates. All rights reserved. *-xguo0<@

 This program is free software: you can redistribute it and/or modify it under the terms of the
 GNU Affero General Public License, version 3, as published by the Free Software Foundation.

 You should have received a copy of the GNU Affero General Public License along with this program.
 If not, see <https://www.gnu.org/licenses/agpl-3.0.txt>
-------------------------------------------------------------------------------------------------*/
package com.antsdb.saltedfish.sql.vdm;

import java.util.List;

import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.HumpbackError;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.sql.planner.SortKey;
import com.antsdb.saltedfish.util.CodingError;

public class RecordLocker extends CursorMaker {
    CursorMaker upstream;
    GTable gtable;
	TableMeta table;
    int[] mapping;
    
    class MyCursor extends CursorWithHeap {
        VdmContext ctx;
        Cursor upstream;
        
        public MyCursor(VdmContext ctx, Cursor upstream) {
            super(upstream.getMetadata());
            this.upstream = upstream;
            this.ctx = ctx;
        }

        @Override
        public long next() {
            long pRecord = this.upstream.next();
            if (pRecord == 0) {
            	return 0;
            }
            long pKey = Record.getKey(pRecord);
            if (pKey != 0) {
            	int timeout = ctx.getSession().getLockTimeoutMilliSeconds();
                Transaction trx = ctx.getTransaction();
            	HumpbackError error = gtable.lock(trx.getGuaranteedTrxId(), pKey, timeout);
            	if (error != HumpbackError.SUCCESS) {
            		throw new OrcaException(error);
            	}
            	// get the latest version of the row, required by select for update
                Row row = gtable.getRow(trx.getTrxId(), Long.MAX_VALUE, pKey);
                pRecord = newRecord();
                Record.setKey(pRecord, row.getKeyAddress());
                for (int i=0; i<this.meta.getColumnCount(); i++) {
                	long pValue = row.getFieldAddress(RecordLocker.this.mapping[i]);
                	Record.set(pRecord, i, pValue);
                }
            }
            return pRecord;
        }

        @Override
        public void close() {
        	super.close();
            this.upstream.close();
        }

    }
    
    public RecordLocker(CursorMaker upstream, TableMeta table, GTable gtable) {
        super();
        if (gtable == null) {
            // record locker must work on a direct table cursor
            throw new CodingError("record locker must work on a direct table cursor");
        }
        this.upstream = upstream;
        this.gtable = gtable;
        this.table = table;
        this.mapping = this.getCursorMeta().getHumpbackMapping();
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        Cursor c = this.upstream.make(ctx, params, pMaster);
        c = new MyCursor(ctx, c);
        return c;
    }

    @Override
    public CursorMeta getCursorMeta() {
        return this.upstream.getCursorMeta();
    }

    @Override
    public boolean setSortingOrder(List<SortKey> order) {
        return this.upstream.setSortingOrder(order);
    }

}
