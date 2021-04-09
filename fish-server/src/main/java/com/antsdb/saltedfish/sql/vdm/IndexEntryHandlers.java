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

import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.HumpbackSession;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.VaporizingRow;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.meta.IndexMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;

/**
 * container of IndexEntryHandler
 *  
 * @author wgu0
 */
public final class IndexEntryHandlers {
    List<IndexEntryHandler> handlers = new ArrayList<>();
    
    IndexEntryHandlers(Orca orca, TableMeta table) {
            Humpback humpback = orca.getHumpback();
            for (IndexMeta i:table.getIndexes()) {
                IndexEntryHandler handler;
                GTable gindex = humpback.getTable(i.getIndexTableId());
                if (i.isFullText()) {
                    handler = new FullTextIndexEntryHandler(gindex, table, i);
                } 
                else {
                    handler = new IndexEntryHandler(gindex, table, i);
                }
                handlers.add(handler);
                handler.keyMaker = i.getKeyMaker();
            }
    }
    
    void insert(Heap heap, 
                HumpbackSession hsession, 
                Transaction trx, 
                VaporizingRow row, 
                int timeout, 
                boolean isReplace) {
        for (IndexEntryHandler i:this.handlers) {
            i.insert(heap, hsession, trx, row, timeout, isReplace);
        }
    }
    
    void delete(Heap heap, HumpbackSession hsession, Transaction trx, Row row, int timeout) {
        for (IndexEntryHandler i:this.handlers) {
            i.delete(heap, hsession, trx, row, timeout);
        }
    }

    void update(Heap heap, 
                HumpbackSession hsession, 
                Transaction trx, 
                Row oldRow, 
                VaporizingRow newRow, 
                boolean force, 
                int timeout) {
        for (IndexEntryHandler i:this.handlers) {
            i.update(heap, hsession, trx, oldRow, newRow, force, timeout);
        }
    }
    
    boolean isEmpty() {
        return this.handlers.size() == 0;
    }
    
    /**
     * only works on unique index
     * 
     * @param heap
     * @param trx
     * @param row
     * @return
     */
    long getRowKey(Heap heap, Transaction trx, VaporizingRow row) {
        for (IndexEntryHandler i:this.handlers) {
            if (!i.index.isUnique()) {
                continue;
            }
            long pRowKey = i.getRowKey(heap, trx, row);
            if (pRowKey != 0) {
                return pRowKey;
            }
        }
        return 0;
    }
}
