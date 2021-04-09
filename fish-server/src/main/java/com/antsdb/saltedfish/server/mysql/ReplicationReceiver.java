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
import com.antsdb.saltedfish.beluga.StorageReplicationMark;
import com.antsdb.saltedfish.nosql.CommitEntry;
import com.antsdb.saltedfish.nosql.DeleteEntry2;
import com.antsdb.saltedfish.nosql.DeleteRowEntry2;
import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.Gobbler;
import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.IndexEntry2;
import com.antsdb.saltedfish.nosql.InsertEntry2;
import com.antsdb.saltedfish.nosql.LogEntry;
import com.antsdb.saltedfish.nosql.MessageEntry2;
import com.antsdb.saltedfish.nosql.PutEntry2;
import com.antsdb.saltedfish.nosql.RollbackEntry;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.RowUpdateEntry2;
import com.antsdb.saltedfish.nosql.UpdateEntry2;
import com.antsdb.saltedfish.nosql.Gobbler.EntryType;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.Session;

/**
 * 
 * @author *-xguo0<@
 */
class ReplicationReceiver {
    /** Committed log pointer from replicator of the master */  
    private long lpMasterMark;
    private long lpLocalMark;
    Humpback humpback;
    private Session session;
    private Orca orca;
    
    ReplicationReceiver(Session session) {
        this.session = session;
        this.humpback = session.getOrca().getHumpback();
        this.orca = session.getOrca();
    }
    
    void replicate(MysqlSession session, PacketLogReplicate request) throws Exception {
        long result = 0;
        String resultMsg = null;
        
        // dispatch
        long pLogEntry = request.getLogEntryPointer();
        LogEntry entry = getEntry(request.getLogPointer(), pLogEntry);
        if (entry instanceof InsertEntry2) {
            replicateRow(session, (RowUpdateEntry2)entry);
        }
        else if (entry instanceof PutEntry2) {
            replicateRow(session, (RowUpdateEntry2)entry);
        }
        else if (entry instanceof UpdateEntry2) {
            replicateRow(session, (RowUpdateEntry2)entry);
        }
        else if (entry instanceof DeleteRowEntry2) {
            replicateRow(session, (DeleteRowEntry2)entry);
        }
        else if (entry instanceof IndexEntry2) {
            index(session, (IndexEntry2)entry);
        }
        else if (entry instanceof DeleteEntry2) {
            delete(session, (DeleteEntry2)entry);
        }
        else if (entry instanceof CommitEntry) {
            commit((CommitEntry)entry);
        }
        else if (entry instanceof RollbackEntry) {
            rollback((RollbackEntry)entry);
        }
        else if (entry instanceof MessageEntry2) {
            MessageEntry2 msg = (MessageEntry2) LogEntry.getEntry(0, pLogEntry);
            resultMsg = msg.getMessage();
        }
        else if (entry instanceof StorageReplicationMark) {
            mark((StorageReplicationMark)entry);
        }
        
        // leave a mark as the start point of replication when this node is elected as leader
        if (!(entry instanceof StorageReplicationMark) && this.lpLocalMark == 0) {
            this.lpLocalMark = this.humpback.getSpaceManager().getAllocationPointer();
            this.lpMasterMark = entry.getSpacePointer();
        }
        
        // send response back
        final String temp = resultMsg;
        session.encoder.writePacket(session.out, (packet)->{
            session.encoder.writeOKBody(packet, result, 0, temp, session.session);
        });
    }

    private void rollback(RollbackEntry entry) {
        Gobbler gobbler = this.humpback.getGobbler();
        if (gobbler != null) {
            gobbler.logRollback(this.session.getHSession(), entry.getTrxid());
        }
        this.humpback.getTrxMan().rollback(entry.getTrxid());
    }

    private void commit(CommitEntry entry) {
        Gobbler gobbler = this.humpback.getGobbler();
        if (gobbler != null) {
            gobbler.logCommit(this.session.getHSession(), entry.getTrxid(), entry.getVersion());
        }
        this.humpback.getTrxMan().commit(entry.getTrxid(), entry.getVersion());
    }

    private LogEntry getEntry(long lpLogEntry, long pLogEntry) {
        EntryType type = LogEntry.getType(pLogEntry);
        if (type == EntryType.OTHER) {
            return new StorageReplicationMark(pLogEntry);
        }
        else {
            return LogEntry.getEntry(lpLogEntry, pLogEntry);
        }
    }

    private void mark(StorageReplicationMark entry) {
        long mark = entry.getLogPointer();
        if (this.lpLocalMark != 0 && mark >= this.lpMasterMark) {
            this.orca.getBelugaPod().setStartReplicationLogPointer(this.lpLocalMark);
            this.lpLocalMark = 0;
            this.lpMasterMark = 0;
        }
    }

    private void delete(MysqlSession session, DeleteEntry2 entry) {
        int tableId = entry.getTableId();
        GTable gtable = humpback.getTable(tableId);
        long lpClone = this.humpback.getGobbler().logClone(entry);
        gtable.getMemTable().recover(lpClone);
    }

    private void index(MysqlSession session, IndexEntry2 entry) {
        int tableId = entry.getTableId();
        GTable gtable = humpback.getTable(tableId);
        long lpClone = this.humpback.getGobbler().logClone(entry);
        gtable.getMemTable().recover(lpClone);
    }

    private void replicateRow(MysqlSession session, RowUpdateEntry2 entry) throws Exception {
        int tableId = entry.getTableId();
        if (tableId == Humpback.SYSSTATS_TABLE_ID) {
            // we dont want to replicate table statistics 
            return;
        }
        GTable gtable = this.humpback.getTable(tableId);
        long lpClone = this.humpback.getGobbler().logClone(entry);
        gtable.getMemTable().recover(lpClone);
        syncSystemTables(session, entry);
    }

    private void syncSystemTables(MysqlSession session, RowUpdateEntry2 entry) throws Exception {
        int tableId = entry.getTableId();
        if (tableId >= 0 && tableId < 0x100) {
            long pRow = entry.getRowPointer();
            long pKey = Row.getKeyAddress(pRow);
            boolean isDelete = entry instanceof DeleteRowEntry2;
            Row row = Row.fromMemoryPointer(pRow, 0);
            session.session.getOrca().syncLocal(entry.getTableId(), pKey, row, isDelete);
        }
    }
}
