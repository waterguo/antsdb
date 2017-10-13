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

import java.util.ArrayList;
import java.util.List;

import com.antsdb.saltedfish.cpp.FishBoundary;
import com.antsdb.saltedfish.cpp.FlexibleHeap;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.sql.LockLevel;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.meta.TableMeta;

public class DeleteSingleRow extends DeleteBase {
    Operator key;
    int tableId;
    
    public DeleteSingleRow(Orca orca, TableMeta table, GTable gtable, Operator key) {
        super(orca, table, gtable);
        this.key = key;
        this.tableId = table.getId();
    }
    
    @Override
    public Object run(VdmContext ctx, Parameters params) {
		ctx.getSession().lockTable(this.tableId, LockLevel.SHARED, true);
		Transaction trx = ctx.getTransaction();
    	try (Heap heap = new FlexibleHeap()) {
    		long pBoundary = this.key.eval(ctx, heap, params, 0);
	        if (pBoundary == 0) {
	            return 0;
	        }
	        FishBoundary boundary = new FishBoundary(pBoundary);
	        long pKey = boundary.getKeyAddress();
	        if (pKey == 0) {
	        	return 0;
	        }
	        Row row = this.gtable.getRow(trx.getTrxId(), trx.getTrxTs(), pKey);
	        if (row == null) {
	            return 0;
	        }
            return deleteSingleRow(ctx, params, pKey) ? 1 : 0;
    	}
    }

    @Override
    List<TableMeta> getDependents() {
        List<TableMeta> list = new ArrayList<TableMeta>();
        list.add(this.table);
        return list;
    }

}
