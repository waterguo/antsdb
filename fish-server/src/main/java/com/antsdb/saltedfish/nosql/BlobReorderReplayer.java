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
import java.util.List;

import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.cpp.Value;
import com.antsdb.saltedfish.nosql.Gobbler.InsertEntry;
import com.antsdb.saltedfish.nosql.Gobbler.LogEntry;
import com.antsdb.saltedfish.nosql.Gobbler.PutEntry;
import com.antsdb.saltedfish.nosql.Gobbler.RowUpdateEntry;
import com.antsdb.saltedfish.nosql.Gobbler.UpdateEntry;

/**
 * reorder the rows so that blob record is following the master record right behind
 * 
 * @author *-xguo0<@
 */
public class BlobReorderReplayer extends ReplayRelay {
    List<Entry> buf = new ArrayList<>();
    
    private static class Entry {
        long p;
        long lp;
        
        Entry(long p, long lp) {
            this.p = p;
            this.lp = lp;
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
    public void update(UpdateEntry entry) throws Exception {
        handle(entry);
    }

    @Override
    public void put(PutEntry entry) throws Exception {
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
        long pRow = entry.getRowPointer();
        Row row = Row.fromMemoryPointer(pRow, 0);
        if (hasBlobRef(row)) {
            buf.add(new Entry(entry.getAddress(), entry.getSpacePointer()));
            return;
        }
        else {
            for (int i=0; i<this.buf.size(); i++) {
                Entry ii = this.buf.get(i);
                LogEntry parent = LogEntry.getEntry(ii.lp, ii.p);
                if (isPair((RowUpdateEntry) parent, entry)) {
                    replay(parent);
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
        else {
            throw new IllegalArgumentException();
        }
    }

    private boolean isPair(RowUpdateEntry parent, RowUpdateEntry blob) {
        if (parent.getType() != blob.getType()) {
            return false;
        }
        if (parent.getTrxId() != blob.getTrxId()) {
            return false;
        }
        if ((parent.getTableId() + 1) != blob.getTableId()) {
            return false;
        }
        Row rowParent = Row.fromMemoryPointer(parent.getRowPointer(), 0);
        Row rowBlob = Row.fromMemoryPointer(blob.getRowPointer(), 0);
        if (KeyBytes.compare(rowParent.getKeyAddress(), rowBlob.getKeyAddress()) != 0) {
            return false;
        }
        return true;
    }

    public long getLogPointer() {
        long result = Long.MAX_VALUE;
        for (Entry i:this.buf) {
            long lpParentEntry = i.lp;
            result = Math.min(result, lpParentEntry);
        }
        return result;
    }
}
