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

import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.HumpbackError;
import com.antsdb.saltedfish.nosql.HumpbackSession;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.VaporizingRow;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.meta.IndexMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * manipulates index at entry level
 * 
 * @author wgu0
 */
public class IndexEntryHandler {
    static Logger _log = UberUtil.getThisLogger();
    
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
    
    void insert(Heap heap, 
                HumpbackSession hsession, 
                Transaction trx, 
                VaporizingRow row, 
                int timeout, 
                boolean isReplace) {
            long pIndexKey = keyMaker.make(heap, row);
            long pRowKey = row.getKeyAddress();
            insert(heap, hsession, trx, pIndexKey, pRowKey, (byte)0, timeout, isReplace, null);
    }
    
    void insert(Heap heap, 
                HumpbackSession hsession, 
                Transaction trx, 
                long pIndexKey, 
                long pRowKey, 
                byte misc, 
                int timeout, 
                boolean isReplace,
                Row oldRow) {
        for (;;) {
            long trxid = trx.getTrxId();
            long trxts = trx.getTrxTs();
            HumpbackError error = this.gtable.insertIndex(hsession, trxid, pIndexKey, pRowKey, misc, timeout);
            if (error == HumpbackError.SUCCESS) {
                return;
            }
            else {
                String loc = this.gtable.getMemTable().getLocation(trxid, trxts, pIndexKey);
                _log.debug("location: {}", loc);
                long pIndex = this.gtable.getMemTable().getIndex(trxid, trxts, pIndexKey);
                _log.debug("pIndex: {}", pIndex);
                Long version = (oldRow == null) ? null : oldRow.getVersion();
                String msg;
                if (error == HumpbackError.MISSING) {
                    msg = "index entry not found";
                }
                else if (error == HumpbackError.LOCK_COMPETITION) {
                    msg = "index entry is locked by another session";
                }
                else if (error == HumpbackError.EXISTS) {
                    msg = "unique index violation";
                }
                else {
                    msg = error.toString();
                }
                throw new OrcaException(
                    "{} tableId={} rowkey={} indexkey={} old_version={}", 
                    msg, 
                    this.gtable.getId(),
                    KeyBytes.toString(pRowKey), 
                    KeyBytes.toString(pIndexKey),
                    version);
            }
        }
    }
    
    void delete(Heap heap, HumpbackSession hsession, Transaction trx, Row row, int timeout) {
        long pIndexKey = keyMaker.make(heap, row);
        delete(heap, hsession, trx, pIndexKey, row, timeout);
    }
    
    void delete(Heap heap, 
                HumpbackSession hsession, 
                Transaction trx, 
                long pIndexKey, 
                Row row, 
                int timeout) {
        HumpbackError error = this.gtable.delete(hsession, trx.getTrxId(), pIndexKey, timeout);
        if (error != HumpbackError.SUCCESS) {
            String msg;
            if (error == HumpbackError.MISSING) {
                msg = "index entry not found";
            }
            else if (error == HumpbackError.LOCK_COMPETITION) {
                msg = "index entry is locked by another session";
            }
            else {
                msg = error.toString();
            }
            throw new OrcaException(
                    "{} index={} tableId={} rowkey={} indexkey={} old_version={}", 
                    msg, 
                    this.index.getName(),
                    this.gtable.getId(),
                    KeyBytes.toString(row.getKeyAddress()), 
                    KeyBytes.toString(pIndexKey),
                    row.getVersion());
        }
    }

    void update(Heap heap, 
                HumpbackSession hsession, 
                Transaction trx, 
                Row oldRow, 
                VaporizingRow newRow, 
                boolean force, 
                int timeout) {
        long pOldIndexKey = this.keyMaker.make(heap, oldRow);
        long pNewIndexKey = this.keyMaker.make(heap, newRow);
        if (!force) {
            if (KeyMaker.equals(pOldIndexKey, pNewIndexKey)) {
                return;
            }
        }
        long pRowKey = newRow.getKeyAddress();
        delete(heap, hsession, trx, pOldIndexKey, oldRow, timeout);
        insert(heap, hsession, trx, pNewIndexKey, pRowKey, (byte)0, timeout, true, oldRow);
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
