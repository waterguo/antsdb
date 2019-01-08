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
import com.antsdb.saltedfish.nosql.Gobbler.InsertEntry;
import com.antsdb.saltedfish.nosql.Gobbler.InsertEntry2;
import com.antsdb.saltedfish.nosql.Gobbler.LogEntry;
import com.antsdb.saltedfish.nosql.Gobbler.PutEntry;
import com.antsdb.saltedfish.nosql.Gobbler.PutEntry2;
import com.antsdb.saltedfish.nosql.Gobbler.RowUpdateEntry;
import com.antsdb.saltedfish.nosql.Gobbler.RowUpdateEntry2;
import com.antsdb.saltedfish.nosql.Gobbler.TransactionWindowEntry;
import com.antsdb.saltedfish.nosql.Gobbler.UpdateEntry;
import com.antsdb.saltedfish.nosql.Gobbler.UpdateEntry2;

/**
 * reorder the rows so that blob record is following the master record right behind
 * 
 * @author *-xguo0<@
 */
public class BlobReorderReplayer extends ReplayRelay {
    List<Entry> buf = new ArrayList<>();
    
    private static class Entry {
        long pEntry;
        long sp;
        long trxId;
        long tableId;
        long pRow;
        EntryType type;
        
        Entry(long p, long lp, long trxId, long tableId, long pRow, EntryType type) {
            this.pEntry = p;
            this.sp = lp;
            this.trxId = trxId;
            this.tableId = tableId;
            this.pRow = pRow;
            this.type = type;
        }
    }
    
    public BlobReorderReplayer(ReplayHandler downstream) {
        super(downstream);
    }
    
    @Override
    public void insert(InsertEntry entry) throws Exception {
        handle(entry);
    }

    @Override
    public void insert(InsertEntry2 entry) throws Exception {
        handle(entry);
    }

    @Override
    public void update(UpdateEntry entry) throws Exception {
        handle(entry);
    }

    @Override
    public void update(UpdateEntry2 entry) throws Exception {
        handle(entry);
    }

    @Override
    public void put(PutEntry entry) throws Exception {
        handle(entry);
    }

    @Override
    public void put(PutEntry2 entry) throws Exception {
        handle(entry);
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

    private void handle(RowUpdateEntry entry) throws Exception {
        handle(entry, entry.getTrxId(), entry.getTableId(), entry.getRowPointer());
    }
    
    private void handle(RowUpdateEntry2 entry) throws Exception {
        handle(entry, entry.getTrxId(), entry.getTableId(), entry.getRowPointer());
    }
    
    private void handle(LogEntry entry, long trxId, int tableId, long pRow) throws Exception {
        long sp = entry.getSpacePointer();
        Row row = Row.fromMemoryPointer(pRow, 0);
        if (hasBlobRef(row)) {
            buf.add(new Entry(entry.addr, sp, trxId, tableId, pRow, entry.getType()));
            return;
        }
        else {
            for (int i=0; i<this.buf.size(); i++) {
                Entry ii = this.buf.get(i);
                if (isPair(ii, entry.getType(), trxId, tableId, row.getKeyAddress())) {
                    replay(LogEntry.getEntry(ii.sp, ii.pEntry));
                    this.buf.remove(i);
                    break;
                }
            }
            replay(entry);
        }
    }

    private void replay(LogEntry entry) throws Exception {
        if (entry instanceof InsertEntry) {
            this.downstream.insert((InsertEntry)entry);
        }
        else if (entry instanceof UpdateEntry) {
            this.downstream.update((UpdateEntry)entry);
        }
        else if (entry instanceof PutEntry) {
            this.downstream.put((PutEntry)entry);
        }
        else if (entry instanceof InsertEntry2) {
            this.downstream.insert((InsertEntry2)entry);
        }
        else if (entry instanceof UpdateEntry2) {
            this.downstream.update((UpdateEntry2)entry);
        }
        else if (entry instanceof PutEntry2) {
            this.downstream.put((PutEntry2)entry);
        }
        else {
            throw new IllegalArgumentException();
        }
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
        long result = Long.MAX_VALUE;
        for (Entry i:this.buf) {
            long lpParentEntry = i.sp;
            result = Math.min(result, lpParentEntry);
        }
        return result;
    }

    @Override
    public void transactionWindow(TransactionWindowEntry entry) throws Exception {
        this.downstream.transactionWindow(entry);
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
    }
}
