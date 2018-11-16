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
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.codec.Charsets;
import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.cpp.Value;

import static com.antsdb.saltedfish.util.UberFormatter.*;

import com.antsdb.saltedfish.util.LatencyDetector;
import com.antsdb.saltedfish.util.UberFormatter;
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
    } 
    
    public static class LogEntry {
        private final static int HEADER_SIZE = 6;
        protected final static int OFFSET_MAGIC = 0;
        protected final static int OFFSET_ENTRY_TYPE = 2;
        protected final static int OFFSET_SIZE = 3;
        protected final static int OFFSET_TRX_ID = 6;
        protected final static int OFFSET_TABLE_ID = 0xe;
        
        protected long sp;
        protected long addr;
        
        LogEntry(SpaceManager sm, int size) {
            this.sp = sm.alloc(HEADER_SIZE + size);
            this.addr = sm.toMemory(this.sp);
            LatencyDetector.run(_log, "setSize", ()->{ 
                setSize(size);
                return null;
            });
        }
        
        public LogEntry(long sp, long addr) {
            this.sp = sp;
            this.addr = addr;
        }
        
        protected void finish(EntryType type) {
            setType(type);
            setMagic();
        }
        
        final short getMagic() {
            return Unsafe.getShortVolatile(addr + OFFSET_MAGIC);
        }
        
        final void setMagic() {
            Unsafe.putShortVolatile(addr + OFFSET_MAGIC, MAGIC);
        }
        
        public static final LogEntry getEntry(long sp, long p) {
            Gobbler.EntryType type = new LogEntry(sp, p).getType();
            LogEntry result;
            switch (type) {
                case INSERT:
                    result = new InsertEntry(sp, p);
                    break;
                case UPDATE:
                    result = new UpdateEntry(sp, p);
                    break;
                case PUT:
                    result = new PutEntry(sp, p);
                    break;
                case DELETE:
                    result = new DeleteEntry(sp, p);
                    break;
                case DELETE_ROW:
                    result = new DeleteRowEntry(sp, p);
                    break;
                case COMMIT:
                    result = new CommitEntry(sp, p);
                    break;
                case ROLLBACK:
                    result = new RollbackEntry(sp, p);
                    break;
                case INDEX:
                    result = new IndexEntry(sp, p);
                    break;
                case MESSAGE:
                    result = new MessageEntry(sp, p);
                    break;
                case TRXWINDOW:
                    result = new TransactionWindowEntry(sp, p);
                    break;
                case TIMESTAMP:
                    result = new TimestampEntry(sp, p);
                    break;
                default:
                    throw new IllegalArgumentException();
            }
            return result;
        }
        
        public final EntryType getType() {
            byte bt = Unsafe.getByteVolatile(addr + OFFSET_ENTRY_TYPE);
            switch (bt) {
            case 0:
                return EntryType.INSERT;
            case 1:
                return EntryType.UPDATE;
            case 2:
                return EntryType.PUT;
            case 3:
                return EntryType.INDEX;
            case 4:
                return EntryType.DELETE;
            case 5:
                return EntryType.COMMIT;
            case 6:
                return EntryType.ROLLBACK;
            case 7:
                return EntryType.MESSAGE;
            case 8:
                return EntryType.TRXWINDOW;
            case 9:
                return EntryType.TIMESTAMP;
            case 10:
                return EntryType.DELETE_ROW;
            default:
                throw new IllegalArgumentException();
            }
        }
        
        final void setType(EntryType type) {
            byte bt = (byte)type.ordinal();
            Unsafe.putByteVolatile(addr + OFFSET_ENTRY_TYPE, bt);
        }
        
        public final int getSize() {
            return Unsafe.getInt3Volatile(addr + OFFSET_SIZE);
        }
        
        final void setSize(int size) {
            Unsafe.putInt3Volatile(addr + OFFSET_SIZE, size);
        }

        public final long getAddress() {
            return this.addr;
        }
        
        public final long getSpacePointer() {
            return this.sp;
        }
    }
    
    public static class TimestampEntry extends LogEntry {
        
        TimestampEntry(SpaceManager sm, long time) {
            super(sm, 8);
            setTimestamp(time);
            finish(EntryType.TIMESTAMP);
        }
        
        protected TimestampEntry(long sp, long addr) {
            super(sp, addr);
        }
        
        private void setTimestamp(long value) {
            Unsafe.putLong(this.addr + OFFSET_TRX_ID, value);
        }

        public long getTimestamp() {
            return Unsafe.getLong(this.addr + OFFSET_TRX_ID);
        }
        
        @Override
        public String toString() {
            long ts = getTimestamp();
            return Instant.ofEpochMilli(ts).atZone(ZoneId.systemDefault()).toLocalDateTime().toString();
        }
    }
    
    public static class RowUpdateEntry extends LogEntry {
        protected final static int OFFSET_TABLE_ID = 6;
        
        RowUpdateEntry(SpaceManager sm, int rowsize, int tableId) {
            super(sm, 4 + rowsize);
            setTableId(tableId);
        }
        
        protected RowUpdateEntry(long sp, long addr) {
            super(sp, addr);
        }
            
        public static long getHeaderSize() {
            return OFFSET_TABLE_ID + 4;
        }
        
        public long getRowPointer() {
            return this.addr + OFFSET_TABLE_ID + 4;            
        }

        public long getRowSpacePointer() {
            return this.sp + OFFSET_TABLE_ID + 4;
        }
        
        public long getTrxId() {
            long pRow = getRowPointer();
            long trxid = Row.getVersion(pRow);
            return trxid;
        }

        public int getTableId() {
            return Unsafe.getInt(this.addr + OFFSET_TABLE_ID);
        }
        
        void setTableId(int tableId) {
            Unsafe.putInt(this.addr + OFFSET_TABLE_ID, tableId);
        }

        @Override
        public String toString() {
            StringBuilder buf = new StringBuilder();
            buf.append("sp=");
            buf.append(hex(this.getRowSpacePointer()));
            buf.append(" ");
            buf.append("trxid=");
            buf.append(this.getTrxId());
            return buf.toString();
        }
    }
    
    public static class InsertEntry extends RowUpdateEntry {
        InsertEntry(SpaceManager sm, VaporizingRow row, int tableId, long start) {
            super(sm, row.getSize(), tableId);
            LatencyDetector.run(_log, "Row.from", ()->{ 
                Row.from(getRowPointer(), row);
                return null;
            });
            finish(EntryType.INSERT);
        }
        
        InsertEntry(long sp, long addr) {
            super(sp, addr);
        }
    }
    
    public static class UpdateEntry extends RowUpdateEntry {
        UpdateEntry(SpaceManager sm, VaporizingRow row, int tableId) {
            super(sm, row.getSize(), tableId);
            Row.from(getRowPointer(), row);
            finish(EntryType.UPDATE);
        }
        
        UpdateEntry(long sp, long addr) {
            super(sp, addr);
        }
    }
    
    public static class PutEntry extends RowUpdateEntry {
        PutEntry(SpaceManager sm, long pRow, int size, int tableId) {
            super(sm, size, tableId);
            Unsafe.copyMemory(pRow, getRowPointer(), size);
            finish(EntryType.PUT);
        }
        
        PutEntry(SpaceManager sm, VaporizingRow row, int tableId) {
            super(sm, row.getSize(), tableId);
            Row.from(getRowPointer(), row);
            finish(EntryType.PUT);
        }
        
        PutEntry(long sp, long addr) {
            super(sp, addr);
        }
    }
    
    public final static class DeleteRowEntry extends RowUpdateEntry {
        DeleteRowEntry(SpaceManager sm, long pRow, int tableId, long version) {
            super(sm, Row.getLength(pRow), tableId);
            long pData = getRowPointer();
            Unsafe.copyMemory(pRow, pData, Row.getLength(pRow));
            Row.setVersion(pData, version);
            finish(EntryType.DELETE_ROW);
        }
        
        DeleteRowEntry(long sp, long addr) {
            super(sp, addr);
        }
    }
    
    public final static class DeleteEntry extends LogEntry {
        protected final static int OFFSET_KEY = 0x12;

        DeleteEntry(SpaceManager sm, int tableId, long trxid, long pKey, int length) {
            super(sm, 4 + 8 + length);
            setTrxId(trxid);
            setTableId(tableId);
            Unsafe.copyMemory(pKey, getKeyAddress(), length);
            finish(EntryType.DELETE);
        }
    
        DeleteEntry(long sp, long addr) {
            super(sp, addr);
        }

        public long getTrxid() {
            return Unsafe.getLong(this.addr + OFFSET_TRX_ID);
        }
        
        void setTrxId(long trxid) {
            Unsafe.putLong(this.addr + OFFSET_TRX_ID, trxid);
        }

        public int getTableId() {
            return Unsafe.getInt(this.addr + OFFSET_TABLE_ID);
        }
        
        void setTableId(int tableId) {
            Unsafe.putInt(this.addr + OFFSET_TABLE_ID, tableId);
        }

        public long getKeyAddress() {
            return this.addr + OFFSET_KEY;
        }

        public static long getHeaderSize() {
            return OFFSET_KEY;
        }
    }
    
    public final static class IndexEntry extends LogEntry {
        protected final static int OFFSET_MISC = 0x12;
        protected final static int OFFSET_INDEX_KEY = 0x13;
        
        static IndexEntry alloc(SpaceManager sm, int tableId, long trxid, long pIndexKey, long pRowKey, byte misc) {
            int indexKeySize = KeyBytes.getRawSize(pIndexKey);
            int rowKeySize = (pRowKey != 0) ? KeyBytes.getRawSize(pRowKey) : 1;
            int size = 8 + 4 + 1 + indexKeySize + rowKeySize;
            IndexEntry entry = new IndexEntry(sm, size);
            entry.setTableId(tableId);
            entry.setTrxId(trxid);
            entry.setMisc(misc);
            Unsafe.copyMemory(pIndexKey, entry.getIndexKeyAddress(), indexKeySize);
            if (pRowKey != 0) {
                Unsafe.copyMemory(pRowKey, entry.getIndexKeyAddress() + indexKeySize, rowKeySize);
            }
            else {
                Unsafe.putByte(entry.getIndexKeyAddress() + indexKeySize, Value.FORMAT_NULL);
            }
            return entry;
        }

        private IndexEntry (SpaceManager sm, int size) {
            super(sm, size);
            finish(EntryType.INDEX);
        }
        
        IndexEntry(long sp, long addr) {
            super(sp, addr);
        }
    
        public static long getHeaderSize() {
            return ENTRY_HEADER_SIZE;
        }
        
        public long getIndexLineAddress() {
            return this.addr + OFFSET_MISC;
        }
        
        public long getTrxid() {
            return Unsafe.getLong(this.addr + OFFSET_TRX_ID);
        }
        
        void setTrxId(long trxid) {
            Unsafe.putLong(this.addr + OFFSET_TRX_ID, trxid);
        }

        public int getTableId() {
            return Unsafe.getInt(this.addr + OFFSET_TABLE_ID);
        }
        
        void setTableId(int tableid) {
            Unsafe.putInt(this.addr + OFFSET_TABLE_ID, tableid);
        }

        public long getIndexKeyAddress() {
            return this.addr + OFFSET_INDEX_KEY;
        }
    
        public long getRowKeyAddress() {
            long p = getIndexKeyAddress();
            int indexKeySize = KeyBytes.getRawSize(p);
            long pRowKey = p + indexKeySize;
            if (Value.getFormat(null, pRowKey) == Value.FORMAT_NULL) {
                return 0;
            }
            else {
                return pRowKey;
            }
        }
        
        public byte getMisc() {
            byte value =  Unsafe.getByte(this.addr + OFFSET_MISC);
            return value;
        }
        
        public void setMisc(byte value) {
            Unsafe.putByte(this.addr + OFFSET_MISC, value);
        }
    }
    
    public final static class CommitEntry extends LogEntry {
        protected final static int OFFSET_VERSION = LogEntry.HEADER_SIZE + 8;
        protected final static int OFFSET_SESSION = OFFSET_VERSION + 8;
        
        CommitEntry(SpaceManager sm, long trxid, long trxts, int sessionId) {
            super(sm, 8 + 8 + 4);
            setTrxId(trxid);
            setVersion(trxts);
            setSessionId(sessionId);
            finish(EntryType.COMMIT);
        }
        
        CommitEntry(long sp, long addr) {
            super(sp, addr);
        }

        public long getTrxid() {
            return Unsafe.getLong(this.addr + OFFSET_TRX_ID);
        }
        
        void setTrxId(long trxid) {
            Unsafe.putLong(this.addr + OFFSET_TRX_ID, trxid);
        }

        public long getVersion() {
            return Unsafe.getLong(this.addr + OFFSET_VERSION);
        }
        
        void setVersion(long version) {
            Unsafe.putLong(this.addr + OFFSET_VERSION, version);
        }
        
        public int getSessionId() {
            return Unsafe.getInt(this.addr + OFFSET_SESSION);
        }
        
        void setSessionId(int value) {
            Unsafe.putInt(this.addr + OFFSET_SESSION, value);
        }
    }
    
    public final static class RollbackEntry extends LogEntry {
        protected final static int OFFSET_SESSION = OFFSET_TRX_ID + 8;
        
        RollbackEntry(SpaceManager sm, long trxid, int sessionId) {
            super(sm, 8 + 4);
            setTrxId(trxid);
            setSessionId(sessionId);
            finish(EntryType.ROLLBACK);
        }
        
        RollbackEntry(long sp, long addr) {
            super(sp, addr);
        }

        public long getTrxid() {
            return Unsafe.getLong(this.addr + OFFSET_TRX_ID);
        }
        
        void setTrxId(long trxid) {
            Unsafe.putLong(this.addr + OFFSET_TRX_ID, trxid);
        }
        
        public int getSessionId() {
            return Unsafe.getInt(this.addr + OFFSET_SESSION);
        }
        
        void setSessionId(int value) {
            Unsafe.putInt(this.addr + OFFSET_SESSION, value);
        }
    }
    
    public final static class TransactionWindowEntry extends LogEntry {
        TransactionWindowEntry(SpaceManager sm, long trxid) {
            super(sm, 8);
            setTrxId(trxid);
            finish(EntryType.TRXWINDOW);
        }
        
        TransactionWindowEntry(long sp, long addr) {
            super(sp, addr);
        }

        /**
         * get the oldest closed transaction at the point of the log position. in another word, after this
         * point all trxid > getTrxid()
         * 
         * @return
         */
        public long getTrxid() {
            return Unsafe.getLong(this.addr + OFFSET_TRX_ID);
        }
        
        void setTrxId(long trxid) {
            Unsafe.putLong(this.addr + OFFSET_TRX_ID, trxid);
        }
    }
        
    public final static class MessageEntry extends LogEntry {
        protected final static int OFFSET_MESSAGE = 6;
        
        static MessageEntry alloc(SpaceManager sm, String message) {
            byte[] bytes = message.getBytes(Charsets.UTF_8);
            MessageEntry entry = new MessageEntry(sm, bytes.length);
            long p = entry.addr;
            for (int i=0; i<bytes.length; i++) {
                Unsafe.putByte(p + OFFSET_MESSAGE + i, bytes[i]);
            }
            return entry;
        }
        
        private MessageEntry(SpaceManager sm, int size) {
            super(sm, size);
            finish(EntryType.MESSAGE);
        }
        
        MessageEntry(long sp, long addr) {
            super(sp, addr);
        }

        public String getMessage() {
            int size = getSize();
            byte[] bytes = new byte[size];
            for (int i=0; i<size; i++) {
                bytes[i] = Unsafe.getByte(this.addr + OFFSET_MESSAGE + i);
            }
            return new String(bytes, Charsets.UTF_8);
        }
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
    
    public void logMessage(HumpbackSession hsession, String message) {
        MessageEntry entry = MessageEntry.alloc(spaceman, message);
        long sp = entry.getSpacePointer();
        updatePersistencePointer(sp);
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

    public void logCommit(HumpbackSession hsession, long trxid, long trxts) {
        CommitEntry entry = new CommitEntry(spaceman, trxid, trxts, hsession.getId());
        long sp = entry.getSpacePointer();
        updatePersistencePointer(sp);
        logTimestamp();
    }

    void logRollback(HumpbackSession hsession, long trxid) {
        RollbackEntry entry = new RollbackEntry(spaceman, trxid, hsession.getId());
        long sp = entry.getSpacePointer();
        updatePersistencePointer(sp);
        logTimestamp();
    }

    long logIndex(HumpbackSession hsession, int tableId, long trxid, long pIndexKey, long pRowKey, byte misc) {
        IndexEntry entry = IndexEntry.alloc(spaceman, tableId, trxid, pIndexKey, pRowKey, misc);
        long sp = entry.getSpacePointer();
        updatePersistencePointer(sp);
        logTimestamp();
        return sp + ENTRY_HEADER_SIZE;
    }
    
    long logInsert(HumpbackSession hsession, VaporizingRow row, int tableId) {
        long start = UberTime.getTime();
        InsertEntry entry = new InsertEntry(spaceman, row, tableId, start);
        long sp = entry.getSpacePointer();
        updatePersistencePointer(sp);
        logTimestamp();
        return entry.getRowSpacePointer();
    }
    
    long logUpdate(HumpbackSession hsession, VaporizingRow row, int tableId) {
        UpdateEntry entry = new UpdateEntry(spaceman, row, tableId);
        long sp = entry.getSpacePointer();
        updatePersistencePointer(sp);
        logTimestamp();
        return entry.getRowSpacePointer();
    }
    
    long logPut(HumpbackSession hsession, VaporizingRow row, int tableId) {
        PutEntry entry = new PutEntry(spaceman, row, tableId);
        long sp = entry.getSpacePointer();
        updatePersistencePointer(sp);
        logTimestamp();
        return entry.getRowSpacePointer();
    }
    
    public long logDeleteRow(HumpbackSession hsession, long trxid, int tableId, long pRow) {
        DeleteRowEntry entry = new DeleteRowEntry(spaceman, pRow, tableId, trxid);
        long sp = entry.getSpacePointer();
        updatePersistencePointer(sp);
        logTimestamp();
        return entry.getRowSpacePointer();
    }

    public long logDelete(HumpbackSession hsession, long trxid, int tableId, long pKey, int length) {
        DeleteEntry entry = new DeleteEntry(spaceman, tableId, trxid, pKey, length);
        long sp = entry.getSpacePointer();
        updatePersistencePointer(sp);
        logTimestamp();
        return sp + ENTRY_HEADER_SIZE;
    }

    public long replayFromRowPointer(long spStartRow, ReplayHandler handler, boolean inclusive) throws Exception {
        long spStart = spStartRow - ENTRY_HEADER_SIZE;
        return replay(spStart, inclusive, handler);
    }
    
    public long replay(long spStart, boolean inclusive, ReplayHandler handler) throws Exception {
        return replay(spStart, Long.MAX_VALUE, inclusive, handler); 
    }

    /**
     * 
     * @param spStart
     * @param spEnd
     * @param inclusive
     * @param handler
     * @return end sp
     * @throws Exception
     */
    public long replay(long spStart, long spEnd, boolean inclusive, ReplayHandler handler) throws Exception {
        long end = -1;
        
        if (this.spaceman.getAllocationPointer() <= spStart) {
            // log is empty
            return spStart;
        }
        
        // start point must be valid
        
        long sp = findStart(spStart);
        if (sp == 0) {
            return end;
        }
        long p = this.spaceman.toMemory(sp);
        
        // loop
        
        for (; sp!=-1 & sp<spEnd;) {
            
            // verify signature and move to next segment if necessary
            
            p = this.spaceman.toMemory(sp);
            if (Unsafe.getShort(p) != MAGIC) {
                sp = this.spaceman.nextSegment(sp);
                if (sp == -1) {
                    // end of space
                    return end;
                }
                p = this.spaceman.toMemory(sp);
                if (Unsafe.getShort(p) != MAGIC) {
                    // no data found
                    return end;
                }
            }
            
            // callback
            
            LogEntry e = new LogEntry(sp, p);
            EntryType type = e.getType();
            int length = e.getSize();
            if (_log.isTraceEnabled()) {
                _log.trace("recover type {} length {} @ {}", type, length, sp);
            }
            if (sp >= spStart) {
                if ((sp != spStart) || inclusive) {
                    replayCallback(type, sp, p, handler);
                    end = sp;
                }
            }
            sp = this.spaceman.plus(sp, length + ENTRY_HEADER_SIZE, 2);
            if (sp == Long.MAX_VALUE) {
                // end of space
                break;
            }
        }
        return end;
    }
    
    private void replayCallback(EntryType type, long sp, long p, ReplayHandler handler) throws Exception {
        switch (type) {
            case INSERT: {
                InsertEntry entry = new InsertEntry(sp, p);
                handler.all(entry);
                handler.insert(entry);
                break;
            }
            case UPDATE: {
                UpdateEntry entry = new UpdateEntry(sp, p);
                handler.all(entry);
                handler.update(entry);
                break;
            }
            case PUT: {
                PutEntry entry = new PutEntry(sp, p);
                handler.all(entry);
                handler.put(entry);
                break;
            }
            case DELETE: {
                DeleteEntry entry = new DeleteEntry(sp, p);
                handler.all(entry);
                handler.delete(entry);
                break;
            }
            case DELETE_ROW: {
                DeleteRowEntry entry = new DeleteRowEntry(sp, p);
                handler.all(entry);
                handler.deleteRow(entry);
                break;
            }
            case COMMIT: {
                CommitEntry entry = new CommitEntry(sp, p);
                handler.all(entry);
                handler.commit(entry);
                break;
            }
            case ROLLBACK: {
                RollbackEntry entry = new RollbackEntry(sp, p);
                handler.all(entry);
                handler.rollback(entry);
                break;
            }
            case INDEX: {
                IndexEntry entry = new IndexEntry(sp, p);
                handler.all(entry);
                handler.index(entry);
                break;
            }
            case MESSAGE: {
                MessageEntry entry = new MessageEntry(sp, p);
                handler.all(entry);
                handler.message(entry);
                break;
            }
            case TRXWINDOW: {
                TransactionWindowEntry entry = new TransactionWindowEntry(sp, p);
                handler.all(entry);
                handler.transactionWindow(entry);
                break;
            }
            case TIMESTAMP: {
                TimestampEntry entry = new TimestampEntry(sp, p);
                handler.all(entry);
                handler.timestamp(entry);
                break;
            }
        }
    }

    private long findStart(long spStart) {
        if (spStart >= this.spaceman.getAllocationPointer()) {
            return 0;
        }
        if (spStart == 0) {
            spStart = getStartSp();
        }
        long p = this.spaceman.toMemory(spStart);
        if (p == 0) {
            return 0;
        }
        if (Unsafe.getShort(p) == MAGIC) {
            return spStart;
        }
        if (Unsafe.getShort(p + 1) == MAGIC) {
            return spStart+1;
        }
        if (Unsafe.getShort(p - 1) == MAGIC) {
            return spStart-1;
        }
        throw new IllegalArgumentException(UberFormatter.hex(spStart));
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
        int spaceId = SpaceManager.getSpaceId(sp);
        long spaceStartSp = this.spaceman.getSpaceStartSp(spaceId);
        if (spaceStartSp == sp) {
            // if current space is empty, wait a little
            try {
                Thread.sleep(1000);
            }
            catch (InterruptedException e) {
            }
        }
        AtomicLong result = new AtomicLong(-1);
        try {
            this.replay(spaceStartSp, true, new ReplayHandler(){
                @Override
                public void all(LogEntry entry) {
                    result.set(entry.getSpacePointer());
                }
            });
        }
        catch (Exception ignored) {
        }
        return result.get();
    }
}
