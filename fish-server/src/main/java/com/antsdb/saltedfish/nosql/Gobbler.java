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

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.util.UberTime;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * a new write ahead log implementation
 *  
 * @author wgu0
 */
public class Gobbler {
    static Logger _log = UberUtil.getThisLogger();
    public static short MAGIC = 0x7777;
    final static int DEFAULT_FILE_SIZE = 1024*1024*64;
    public final static int ENTRY_HEADER_SIZE = 6;

    SpaceManager spaceman;
    LogWriter writer;
    private long timestamp = UberTime.getTime();
    private AtomicLong spPersistence = new AtomicLong();
    
    public static enum EntryType {
        INSERT,
        UPDATE,
        PUT,
        INDEX,
        DELETE,
        COMMIT,
        ROLLBACK,
        MESSAGE,
        TRXWINDOW,
        TIMESTAMP,
        DELETE_ROW,
        DELETE2,
        DELETE_ROW2,
        INSERT2,
        UPDATE2,
        PUT2,
        INDEX2,
        MESSAGE2,
        DDL,
        // quick and dirty solution for cluster replication  
        OTHER,
    } 
    
    public Gobbler(SpaceManager spaceman, boolean enableWriter) throws IOException {
        this.spaceman = spaceman;
        if (!this.spaceman.isReadOnly()) {
            logMessage(null, "gobbler start at " + LocalDateTime.now());
            if (enableWriter) {
                this.spPersistence.set(spaceman.getAllocationPointer());
                this.writer = new LogWriter(spaceman, this);
                this.writer.start();
            }
        }
    }

    private void updatePersistencePointer(long spStart) {
        for (;;) {
            long sp = this.spPersistence.get();
            if (spStart >= sp) {
                break;
            }
            if (this.spPersistence.compareAndSet(sp, spStart)) {
                break;
            }
        }
    }

    public void logTimestamp() {
        long now = UberTime.getTime();
        if ((now / 1000) == (this.timestamp / 1000)) {
            return;
        }
        this.timestamp = now;
        TimestampEntry entry = new TimestampEntry(spaceman, now);
        long sp = entry.getSpacePointer();
        updatePersistencePointer(sp);
    }
    
    public long logDdl(HumpbackSession hsession, String ddl) {
        DdlEntry entry = DdlEntry.alloc(spaceman, hsession.id, ddl);
        long sp = entry.getSpacePointer();
        hsession.setLastLp(sp);
        return sp;
    }
    
    public long logMessage(HumpbackSession hsession, String message) {
        int sessionId = (hsession == null) ? 0 : hsession.id;
        MessageEntry2 entry = MessageEntry2.alloc(spaceman, sessionId, message);
        long sp = entry.getSpacePointer();
        updatePersistencePointer(sp);
        return sp;
    }
    
    /**
     * log the latest closed trx id at the point of logging
     * 
     * @param trxid
     */
    public void logTransactionWindow(long trxid) {
        TransactionWindowEntry entry = new TransactionWindowEntry(spaceman, trxid);
        long sp = entry.getSpacePointer();
        updatePersistencePointer(sp);
    }

    public long logCommit(HumpbackSession hsession, long trxid, long trxts) {
        CommitEntry entry = new CommitEntry(spaceman, trxid, trxts, hsession.getId());
        long sp = entry.getSpacePointer();
        hsession.setLastLp(sp);
        updatePersistencePointer(sp);
        logTimestamp();
        return sp;
    }

    public void logRollback(HumpbackSession hsession, long trxid) {
        RollbackEntry entry = new RollbackEntry(spaceman, trxid, hsession.getId());
        long sp = entry.getSpacePointer();
        hsession.setLastLp(sp);
        updatePersistencePointer(sp);
        logTimestamp();
    }

    long logIndex(HumpbackSession hsession, int tableId, long trxid, long pIndexKey, long pRowKey, byte misc) {
        IndexEntry2 entry = IndexEntry2.alloc(spaceman, hsession.id, tableId, trxid, pIndexKey, pRowKey, misc);
        long sp = entry.getSpacePointer();
        hsession.setLastLp(sp);
        updatePersistencePointer(sp);
        logTimestamp();
        return sp;
    }
    
    long logInsert(HumpbackSession hsession, VaporizingRow row, int tableId) {
        InsertEntry2 entry = new InsertEntry2(spaceman, hsession.id, row, tableId);
        long sp = entry.getSpacePointer();
        hsession.setLastLp(sp);
        updatePersistencePointer(sp);
        logTimestamp();
        return sp;
    }
    
    long logUpdate(HumpbackSession hsession, VaporizingRow row, int tableId) {
        UpdateEntry2 entry = new UpdateEntry2(spaceman, hsession.id, row, tableId);
        long sp = entry.getSpacePointer();
        hsession.setLastLp(sp);
        updatePersistencePointer(sp);
        logTimestamp();
        return sp;
    }
    
    long logPut(HumpbackSession hsession, VaporizingRow row, int tableId) {
        PutEntry2 entry = new PutEntry2(spaceman, hsession.id, row, tableId);
        long sp = entry.getSpacePointer();
        hsession.setLastLp(sp);
        updatePersistencePointer(sp);
        logTimestamp();
        return sp;
    }
    
    public long logDeleteRow(HumpbackSession hsession, long trxid, int tableId, long pRow) {
        DeleteRowEntry2 entry = new DeleteRowEntry2(spaceman, hsession.id, pRow, tableId, trxid);
        long sp = entry.getSpacePointer();
        hsession.setLastLp(sp);
        updatePersistencePointer(sp);
        logTimestamp();
        return sp;
    }

    public long logDelete(HumpbackSession hsession, long trxid, int tableId, long pKey, int length) {
        DeleteEntry2 entry = new DeleteEntry2(spaceman, hsession.id, tableId, trxid, pKey, length);
        long sp = entry.getSpacePointer();
        hsession.setLastLp(sp);
        updatePersistencePointer(sp);
        logTimestamp();
        return sp;
    }

    static LogEntry getLogEntry(long lpEntry, long pEntry) {
        LogEntry result = null;
        LogEntry entry = new LogEntry(lpEntry, pEntry);
        EntryType type = entry.getType();
        switch (type) {
            case COMMIT: {
                result = new CommitEntry(lpEntry, pEntry);
                break;
            }
            case ROLLBACK: {
                result = new RollbackEntry(lpEntry, pEntry);
                break;
            }
            case MESSAGE: {
                result = new MessageEntry(lpEntry, pEntry);
                break;
            }
            case TRXWINDOW: {
                result = new TransactionWindowEntry(lpEntry, pEntry);
                break;
            }
            case TIMESTAMP: {
                result = new TimestampEntry(lpEntry, pEntry);
                break;
            }
            case DELETE2: {
                result = new DeleteEntry2(lpEntry, pEntry);
                break;
            }
            case DELETE_ROW2: {
                result = new DeleteRowEntry2(lpEntry, pEntry);
                break;
            }
            case INDEX2: {
                result = new IndexEntry2(lpEntry, pEntry);
                break;
            }
            case INSERT2: {
                result = new InsertEntry2(lpEntry, pEntry);
                break;
            }
            case MESSAGE2: {
                result = new MessageEntry2(lpEntry, pEntry);
                break;
            }
            case PUT2: {
                result = new PutEntry2(lpEntry, pEntry);
                break;
            }
            case UPDATE2: {
                result = new UpdateEntry2(lpEntry, pEntry);
                break;
            }
            case DDL: {
                result = new DdlEntry(lpEntry, pEntry);
                break;
            }
            default:
                break;
        }
        return result;
    }
    
    public void close() {
        _log.info("closing gobbler ...");
        if (this.writer != null) {
            this.writer.close();
            try {
                this.writer.join();
            }
            catch (InterruptedException ignored) {
            }
            this.writer = null;
        }
    }

    public AtomicLong getPersistencePointer() {
        return this.spPersistence;
    }

    /**
     * get earliest possible space pointer
     * 
     * @return
     */
    public long getStartSp() {
        return this.spaceman.getStartSp();
    }

    /**
     * return -1 if there is no valid sp found meaning this is an empty database
     * @throws Exception 
     */
    public long getLatestSp() {
        long sp = this.spaceman.getAllocationPointer();
        return sp;
    }

    /**
     * log a clone of the input entry
     * @param entry not null
     * @return log pointer to the clone
     */
    public long logClone(LogEntry entry) {
        int size = entry.getSize() + LogEntry.HEADER_SIZE;
        long lp = this.spaceman.alloc(size);
        long p = this.spaceman.toMemory(lp);
        Unsafe.copyMemory(entry.getAddress(), p, size);
        updatePersistencePointer(lp);
        logTimestamp();
        return lp;
    }
}
