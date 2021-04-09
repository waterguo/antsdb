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

import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.nosql.Gobbler.EntryType;
import com.antsdb.saltedfish.util.UberFormatter;

/**
 * read the log entries sequentially
 * 
 * @author *-xguo0<@
 */
public class SequentialLogReader {

    private long lpNext;
    private long lpLast;
    private SpaceManager spaceman;
    private Gobbler gobbler;

    public SequentialLogReader(Gobbler gobbler) {
        this.gobbler = gobbler;
        this.spaceman = gobbler.spaceman;
    }

    public void setPosition(long value, boolean inclusive) {
        this.lpLast = -1;
        this.lpNext = findStart(value);
        if (!inclusive) {
            read();
        }
    }
    
    /**
     * get the log pointer of the next entry to be read
     * 
     * @return -1 if end of log space has been reached
     */
    public long getPosition() {
        return this.lpNext;
    }
    
    /**
     * get the log pointer of the last entry read
     * 
     * @return -1 if nothing has been read sine setPosition()
     */
    public long getLast() {
        return this.lpLast;
    }
    
    public void setLast(long value) {
        this.lpLast = value;
    }
    
    public LogEntry read() {
        if (this.lpNext == -1) {
            return null;
        }
        
        // verify signature and move to next segment if necessary
        long p = spaceman.toMemory(this.lpNext);
        if (Unsafe.getShort(p) != Gobbler.MAGIC) {
            long next = this.spaceman.nextSegment(this.lpNext);
            if (next == -1) {
                // end of space
                return null;
            }
            p = spaceman.toMemory(next);
            if (Unsafe.getShort(p) != Gobbler.MAGIC) {
                // no data found
                return null;
            }
            this.lpNext = next;
        }
        
        // read the entry
        LogEntry e = new LogEntry(this.lpNext, p);
        int length = e.getSize();
        LogEntry result = Gobbler.getLogEntry(this.lpNext, p);
        
        // move forward the position pointer 
        this.lpLast = this.lpNext;
        this.lpNext = this.spaceman.plus(this.lpNext, length + Gobbler.ENTRY_HEADER_SIZE, 2);
        if (this.lpNext == Long.MAX_VALUE) this.lpNext = -1;
        
        return result;
    }
    
    public long replay(ReplayHandler handler) throws Exception {
        return replay(Long.MAX_VALUE, handler); 
    }

    /**
     * 
     * @param spEnd
     * @param inclusive
     * @param handler
     * @return end sp
     * @throws Exception
     */
    public long replay(long spEnd, ReplayHandler handler) throws Exception {
        while ((this.lpNext != -1) && (this.lpNext < spEnd)) {
            LogEntry entry = read();
            if (entry == null) break;
            EntryType type = entry.getType();
            handler.all(entry);
            switch (type) {
                case INSERT2:
                    handler.insert((InsertEntry2)entry);
                    break;
                case UPDATE2:
                    handler.update((UpdateEntry2)entry);
                    break;
                case PUT2:
                    handler.put((PutEntry2)entry);
                    break;
                case DELETE2:
                    handler.delete((DeleteEntry2)entry);
                    break;
                case DELETE_ROW2:
                    handler.deleteRow((DeleteRowEntry2)entry);
                    break;
                case COMMIT:
                    handler.commit((CommitEntry)entry);
                    break;
                case ROLLBACK:
                    handler.rollback((RollbackEntry)entry);
                    break;
                case INDEX2:
                    handler.index((IndexEntry2)entry);
                    break;
                case MESSAGE:
                    handler.message((MessageEntry)entry);
                    break;
                case MESSAGE2:
                    handler.message((MessageEntry2)entry);
                    break;
                case TRXWINDOW:
                    handler.transactionWindow((TransactionWindowEntry)entry);
                    break;
                case TIMESTAMP:
                    handler.timestamp((TimestampEntry)entry);
                    break;
                case DDL:
                    handler.ddl((DdlEntry)entry);
                    break;
                default:
                    throw new IllegalArgumentException(String.valueOf(type));
            }
        }
        return this.lpNext;
    }
    
    private long findStart(long spStart) {
        if (spStart >= this.spaceman.getAllocationPointer()) {
            return 0;
        }
        if (spStart == 0) {
            spStart = this.gobbler.getStartSp();
        }
        long p = this.spaceman.toMemory(spStart);
        if (p == 0) {
            return 0;
        }
        if (Unsafe.getShort(p) == Gobbler.MAGIC) {
            return spStart;
        }
        if (Unsafe.getShort(p + 1) == Gobbler.MAGIC) {
            return spStart+1;
        }
        if (Unsafe.getShort(p - 1) == Gobbler.MAGIC) {
            return spStart-1;
        }
        throw new IllegalArgumentException(UberFormatter.hex(spStart));
    }
}
