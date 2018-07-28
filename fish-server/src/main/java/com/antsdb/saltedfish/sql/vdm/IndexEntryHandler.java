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
import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.HumpbackError;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.VaporizingRow;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.meta.IndexMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;

/**
 * manipulates index at entry level
 * 
 * @author wgu0
 */
public class IndexEntryHandler {
    GTable gtable;
    IndexMeta index;
    KeyMaker keyMaker;
    TableMeta table;
    
    public IndexEntryHandler(GTable gindex, TableMeta table, IndexMeta index) {
        this.gtable = gindex;
        this.table = table;
        this.index = index;
        this.keyMaker = index.getKeyMaker();
    }
    
    static List<IndexEntryHandler> create(Orca orca, TableMeta table) {
        List<IndexEntryHandler> handlers = new ArrayList<>();
            Humpback humpback = orca.getHumpback();
            for (IndexMeta i:table.getIndexes()) {
                IndexEntryHandler handler = new IndexEntryHandler(humpback.getTable(i.getIndexTableId()), table, i);
                handlers.add(handler);
            }
        return handlers;
    }
    
    void insert(Heap heap, Transaction trx, VaporizingRow row, int timeout, boolean isReplace) {
            long pIndexKey = keyMaker.make(heap, row);
            long pRowKey = row.getKeyAddress();
            insert(heap, trx, pIndexKey, pRowKey, (byte)0, timeout, isReplace);
    }
    
    void insert(Heap heap, Transaction trx, long pIndexKey, long pRowKey, byte misc, int timeout, boolean isReplace) {
            for (;;) {
            HumpbackError error = this.gtable.insertIndex(trx.getTrxId(), pIndexKey, pRowKey, misc, timeout);
            if (error == HumpbackError.SUCCESS) {
                    return;
            }
            else {
                    error = this.gtable.insertIndex(trx.getTrxId(), pIndexKey, pRowKey, misc, timeout);
                    throw new OrcaException(
                        "{} tableId={} rowkey={} indexkey={}", 
                        error, 
                        this.gtable.getId(),
                        KeyBytes.toString(pRowKey), 
                        KeyBytes.toString(pIndexKey));
            }
            }
    }
    
    void delete(Heap heap, Transaction trx, Row row, int timeout) {
            long pIndexKey = keyMaker.make(heap, row);
            delete(heap, trx, pIndexKey, timeout);
    }
    
    void delete(Heap heap, Transaction trx, long pIndexKey, int timeout) {
            HumpbackError error = this.gtable.delete(trx.getTrxId(), pIndexKey, timeout);
        if (error != HumpbackError.SUCCESS) {
            throw new OrcaException(error);
        }
    }

    void update(Heap heap, Transaction trx, Row oldRow, VaporizingRow newRow, boolean force, int timeout) {
        long pOldIndexKey = this.keyMaker.make(heap, oldRow);
        long pNewIndexKey = this.keyMaker.make(heap, newRow);
        if (!force) {
            if (KeyMaker.equals(pOldIndexKey, pNewIndexKey)) {
                return;
            }
        }
        long pRowKey = newRow.getKeyAddress();
        delete(heap, trx, pOldIndexKey, timeout);
        insert(heap, trx, pNewIndexKey, pRowKey, (byte)0, timeout, true);
    }
    
    long getRowKey(Heap heap, Transaction trx, VaporizingRow row) {
        long pIndexKey = this.keyMaker.make(heap, row);
        return getRowKey(trx, pIndexKey);
    }
    
    long getRowKey(Transaction trx, long pIndexKey) {
        long result = this.gtable.getIndex(trx.getTrxId(), trx.getTrxTs(), pIndexKey);
        return result;
    }
}
