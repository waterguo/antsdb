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
package com.antsdb.saltedfish.server.mysql;

import com.antsdb.mysql.network.PacketLogReplicate;
import com.antsdb.saltedfish.cpp.BluntHeap;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.nosql.DeleteEntry2;
import com.antsdb.saltedfish.nosql.DeleteRowEntry2;
import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.HumpbackError;
import com.antsdb.saltedfish.nosql.HumpbackSession;
import com.antsdb.saltedfish.nosql.IndexEntry2;
import com.antsdb.saltedfish.nosql.InsertEntry2;
import com.antsdb.saltedfish.nosql.LogEntry;
import com.antsdb.saltedfish.nosql.MessageEntry2;
import com.antsdb.saltedfish.nosql.PutEntry2;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.RowUpdateEntry2;
import com.antsdb.saltedfish.nosql.UpdateEntry2;
import com.antsdb.saltedfish.nosql.VaporizingRow;
import com.antsdb.saltedfish.sql.OrcaException;

/**
 * 
 * @author *-xguo0<@
 */
class ReplicationSupport {

    static void replicate(MysqlSession session, PacketLogReplicate request) throws Exception {
        long result = 0;
        String resultMsg = null;
        long pLogEntry = request.getLogEntryPointer();
        LogEntry entry = LogEntry.getEntry(0, pLogEntry);
        if (entry instanceof InsertEntry2) {
            put(session, (RowUpdateEntry2)entry);
        }
        else if (entry instanceof PutEntry2) {
            put(session, (RowUpdateEntry2)entry);
        }
        else if (entry instanceof UpdateEntry2) {
            put(session, (RowUpdateEntry2)entry);
        }
        else if (entry instanceof DeleteRowEntry2) {
            deleteRow(session, (DeleteRowEntry2)entry);
        }
        else if (entry instanceof IndexEntry2) {
            index(session, (IndexEntry2)entry);
        }
        else if (entry instanceof DeleteEntry2) {
            delete(session, (DeleteEntry2)entry);
        }
        else if (entry instanceof MessageEntry2) {
            MessageEntry2 msg = (MessageEntry2) LogEntry.getEntry(0, pLogEntry);
            resultMsg = msg.getMessage();
        }
        final String temp = resultMsg;
        session.encoder.writePacket(session.out, (packet)->{
            session.encoder.writeOKBody(packet, result, 0, temp, session.session);
        });
    }

    private static void delete(MysqlSession session, DeleteEntry2 entry) {
        int tableId = entry.getTableId();
        Humpback humpback = session.session.getOrca().getHumpback();
        GTable gtable = humpback.getTable(tableId);
        try (HumpbackSession hsession=session.session.getHSession().open()) {
            long pKey = entry.getKeyAddress();
            long error = gtable.delete(hsession, 1000, pKey, 0);
            if (!HumpbackError.isSuccess(error)) {
                throw new OrcaException("{} key={}", error, KeyBytes.toString(pKey));
            }
        }
    }

    private static void index(MysqlSession session, IndexEntry2 entry) {
        int tableId = entry.getTableId();
        Humpback humpback = session.session.getOrca().getHumpback();
        GTable gtable = humpback.getTable(tableId);
        try (HumpbackSession hsession=session.session.getHSession().open()) {
            long pIndexKey = entry.getIndexKeyAddress();
            long pRowKey = entry.getRowKeyAddress();
            long error = gtable.insertIndex(hsession, 1000, pIndexKey, pRowKey, entry.getMisc(), 0);
            if (!HumpbackError.isSuccess(error)) {
                throw new OrcaException("{} key={}", error, KeyBytes.toString(pIndexKey));
            }
        }
    }

    private static void deleteRow(MysqlSession session, DeleteRowEntry2 entry) throws Exception {
        long pRow = entry.getRowPointer();
        if (true) {
            int tableId = entry.getTableId();
            Humpback humpback = session.session.getOrca().getHumpback();
            GTable gtable = humpback.getTable(tableId);
            try (HumpbackSession hsession=session.session.getHSession().open()) {
                long error = gtable.deleteRow(hsession, 1000, pRow, 0);
                if (!HumpbackError.isSuccess(error)) {
                    throw new OrcaException("{} key={}", error, KeyBytes.toString(Row.getKeyAddress(pRow)));
                }
            }
        }
        syncSystemTables(session, entry);
    }

    private static void put(MysqlSession session, RowUpdateEntry2 entry) throws Exception {
        long pRow = entry.getRowPointer();
        int tableId = entry.getTableId();
        Humpback humpback = session.session.getOrca().getHumpback();
        GTable gtable = humpback.getTable(tableId);
        try (HumpbackSession hsession=session.session.getHSession().open()) {
            try (Heap heap = new BluntHeap()) {
                VaporizingRow vrow = toVRow(heap, pRow); 
                long error = gtable.put(hsession, vrow, 0);
                if (!HumpbackError.isSuccess(error)) {
                    throw new OrcaException("{} key={}", error, KeyBytes.toString(Row.getKeyAddress(pRow)));
                }
            }
        }
        syncSystemTables(session, entry);
    }

    private static void syncSystemTables(MysqlSession session, RowUpdateEntry2 entry) throws Exception {
        int tableId = entry.getTableId();
        if (tableId >= 0 && tableId < 0x100) {
            long pRow = entry.getRowPointer();
            long pKey = Row.getKeyAddress(pRow);
            boolean isDelete = entry instanceof DeleteRowEntry2;
            Row row = Row.fromMemoryPointer(pRow, 0);
            session.session.getOrca().syncLocal(entry.getTableId(), pKey, row, isDelete);
        }
    }

    private static VaporizingRow toVRow(Heap heap, long pRow) {
        Row row = Row.fromMemoryPointer(pRow, Row.getVersion(pRow));
        VaporizingRow result = new VaporizingRow(heap, row.getMaxColumnId());
        result.setKey(row.getKey());
        result.setVersion(1000);
        for (int i=0; i<=row.getMaxColumnId(); i++) {
            result.setFieldAddress(i, row.getFieldAddress(i));
        }
        return result;
    }
}
