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

import java.util.Iterator;

import com.antsdb.saltedfish.cpp.FlexibleHeap;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.sql.ExternalTable;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.Session;
import com.antsdb.saltedfish.sql.meta.TableMeta;

public class ExternalTableScan extends CursorMaker {
    ObjectName tableName;
    CursorMeta meta;
    
    class MyCursor extends Cursor {
        Iterator<Record> upstream;
		Heap heap;
		long pRecord;
		long heapMark;
        
        public MyCursor() {
            super(ExternalTableScan.this.meta);
			this.heap = new FlexibleHeap();
			this.pRecord = Record.alloc(heap, meta.getColumnCount());
			this.heapMark = this.heap.position();
        }

        @Override
        public long next() {
            if (!this.upstream.hasNext()) {
            	return 0;
            }
            Record rec = this.upstream.next();
			this.heap.reset(this.heapMark);
            Record.set(this.heap, this.pRecord, rec);
            return this.pRecord;
        }

        @Override
        public void close() {
        }

    }
    
    public ExternalTableScan(Session session, ObjectName tableName) {
        this.tableName = tableName;
        TableMeta tableMeta = session.getOrca().getMetaService().getTable(session.getTransaction(), tableName);
        this.meta = CursorMeta.from(tableName, tableMeta);
    }

    @Override
    public CursorMeta getCursorMeta() {
        return meta;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        Orca orca = ctx.getOrca();
        ExternalTable table = orca.getExternalTable(this.tableName.getNamespace(), this.tableName.getTableName());
        MyCursor c = new MyCursor();
        c.upstream = table.scan();
        return c;
    }
}
