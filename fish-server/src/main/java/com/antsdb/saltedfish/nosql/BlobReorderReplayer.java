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
package com.antsdb.saltedfish.nosql;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.cpp.Value;
import com.antsdb.saltedfish.nosql.Gobbler.EntryType;

/**
 * reorder the rows so that blob record is following the master record right behind
 * 
 * @author *-xguo0<@
 */
public class BlobReorderReplayer extends ReplicationHandler2Relay {
    List<Entry> buf = new ArrayList<>();
    volatile long lp = Long.MAX_VALUE;
    
    private static class Entry {
        long pEntry;
        long sp;
        long trxId;
        int tableId;
        long pRow;
        EntryType type;
        
        Entry(long p, long lp, long trxId, int tableId, long pRow, EntryType type) {
            this.pEntry = p;
            this.sp = lp;
            this.trxId = trxId;
            this.tableId = tableId;
            this.pRow = pRow;
            this.type = type;
        }
    }
    
    public BlobReorderReplayer(ReplicationHandler2 downstream) {
        super(downstream);
    }
    
    
    @Override
    public void putRow(int tableId, long pRow, long version, long pEntry, long lpEntry) throws Exception {
        LogEntry entry = new LogEntry(lpEntry, pEntry);
        Row row = Row.fromMemoryPointer(pRow, 0);
        if (hasBlobRef(row)) {
            buf.add(new Entry(pEntry, lpEntry, version, tableId, pRow, entry.getType()));
            updateLp();
        }
        else {
            for (int i=0; i<this.buf.size(); i++) {
                Entry ii = this.buf.get(i);
                if (isPair(ii, entry.getType(), version, tableId, row.getKeyAddress())) {
                    this.downstream.putRow(ii.tableId, ii.pRow, version, ii.pEntry, ii.sp);
                    this.buf.remove(i);
                    updateLp();
                    break;
                }
            }
            this.downstream.putRow(tableId, pRow, version, pEntry, lpEntry);
        }
    }

    private boolean hasBlobRef(Row row) {
        for (int i=0; i<=row.getMaxColumnId(); i++) {
            long pFieldValue = row.getFieldAddress(i);
            byte type = Value.getFormat(null, pFieldValue);
            if ((type == Value.FORMAT_CLOB_REF) || (type == Value.FORMAT_BLOB_REF)) {
                return true;
            }
        }
        return false;
    }

    private boolean isPair(Entry parent, EntryType type, long trxid, int tableId, long pKey) {
        if (parent.type != type) {
            return false;
        }
        if (parent.trxId != trxid) {
            return false;
        }
        if ((parent.tableId + 1) != tableId) {
            return false;
        }
        Row rowParent = Row.fromMemoryPointer(parent.pRow, 0);
        if (KeyBytes.compare(rowParent.getKeyAddress(), pKey) != 0) {
            return false;
        }
        return true;
    }

    public long getLogPointer() {
        return this.lp;
    }

    @Override
    public void transactionWindow(long pEntry, long lpEntry) throws Exception {
        TransactionWindowEntry entry = new TransactionWindowEntry(lpEntry, pEntry);
        this.downstream.transactionWindow(pEntry, lpEntry);
        long oldestTrxid = entry.getTrxid();
        for (Iterator<Entry> i=this.buf.iterator();;) {
            if (!i.hasNext()) {
                break;
            }
            Entry ii = i.next();
            if (ii.trxId > oldestTrxid) {
                i.remove();
            }
        }
        updateLp();
    }
    
    private void updateLp() {
        long minLp = Long.MAX_VALUE;
        for (Entry i:this.buf) {
            minLp = Math.min(minLp, i.sp); 
        }
        this.lp = minLp;
        return;
    }
}
