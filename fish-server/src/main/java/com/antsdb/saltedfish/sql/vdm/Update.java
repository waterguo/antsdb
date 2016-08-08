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

import com.antsdb.saltedfish.cpp.FlexibleHeap;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.sql.LockLevel;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;

public class Update extends UpdateBase {
    CursorMaker maker;
    int tableId;
    
    public Update(Orca orca, 
    		      TableMeta table, 
    		      GTable gtable, 
    		      CursorMaker maker, 
    		      List<ColumnMeta> columns, 
    		      List<Operator> exprs) {
        super(orca, table, gtable, columns, exprs);
        this.maker = maker;
        this.tableId = table.getId();
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
		            long pKey = Record.getKey(pRecord);
		            if (pKey == 0) {
		            	throw new IllegalArgumentException();
		            }
		            heap.reset(0);
		            if (updateSingleRow(ctx, heap, params, pKey)) {
		            	count++;
		            }
		        }
		        return count;
        	}
        }
    }

	@Override
    public List<TableMeta> getDependents() {
        List<TableMeta> list = new ArrayList<TableMeta>();
        list.add(this.table);
        return list;
    }

    @Override
    public void explain(int level, List<ExplainRecord> records) {
        ExplainRecord rec = new ExplainRecord(level, "Update");
        records.add(rec);
        this.maker.explain(level, records);
    }

}
