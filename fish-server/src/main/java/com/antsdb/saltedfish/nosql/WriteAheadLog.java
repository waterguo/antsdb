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

import java.io.EOFException;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;

import com.antsdb.saltedfish.nosql.WriteAheadLogThread.EntryType;
import com.antsdb.saltedfish.util.ChannelReader;
import com.antsdb.saltedfish.util.CodingError;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * See https://en.wikipedia.org/wiki/Write-ahead_logging
 * 
 * @author xguo
 *
 */
class WriteAheadLog {
    static Logger _log = UberUtil.getThisLogger();

    File home;
    
    static class MyFilenameFilter implements FileFilter {
        @Override
        public boolean accept(File pathname) {
            String name = pathname.getName();
            return (name.startsWith("wal-")) && (name.endsWith(".log"));
        }
    }

    static class Commit {
        long trxid;
        long trxts;
        public Commit(long trxid, long trxts) {
            super();
            this.trxid = trxid;
            this.trxts = trxts;
        }
    }
    
    static class Rollback {
        long trxid;
        public Rollback(long trxid) {
            super();
            this.trxid = trxid;
        }
    }

    static class Handler {
        void row(Row row) {
        }
        void commit(long trxid, long trxts) {
        }
        void rollback(long trxid) {
        }
    }
    
    WriteAheadLogThread thread;
    
    WriteAheadLog(File home) throws Exception {
        this.home = home;
        this.thread = new WriteAheadLogThread(home);
        this.thread.start();
    }
    
    void log(Row row) {
        while (this.thread.queue.size() >= 10000) {
            try {
                Thread.sleep(1);
            }
            catch (InterruptedException e) {
            }
        }
        this.thread.queue.offer(row);
    }
    
    void shutdown() {
        this.thread.shutdown();
        /*
        write(WriteAheadLogThread.EOF_MARK);
        while (!this.thread.queue.isEmpty()) {
            try {
                Thread.sleep(10);
            }
            catch (InterruptedException e) {
            }
        }
        while (this.thread.writeCount.get() != 0) {
            try {
                Thread.sleep(10);
            }
            catch (InterruptedException e) {
            }
        }
        */
    }
    
    void checkpoint() throws Exception {
        // recreate the logging thread cuz it will close the logging files properly
        this.thread.shutdown();
        this.thread.logs.forEach(it->{
            _log.info("deleting log file {} ...", it);
            it.delete();
        });
        this.thread.logs.clear();
        this.thread = new WriteAheadLogThread(this.home);
        this.thread.start();
    }

    void logCommit(long trxid, long trxts) {
        this.thread.queue.offer(new Commit(trxid, trxts));
    }
    
    void logRollback(long trxid) {
        this.thread.queue.offer(new Rollback(trxid));
    }
    
    /**
     * iterate the data in the log
     * @throws IOException 
     * 
     */
    void iterate(Handler handler) throws IOException {
        try (ChannelReader in = new ChannelReader(new WriteAheadLogChannel(this.thread.logs))) {
            for (;;) {
                byte type = in.readByte();
                if (type < 0) {
                    break;
                }
                if (type == EntryType.ROW.ordinal()) {
                    int tableId = in.readInt();
                    int size = in.readInt();
                    ByteBuffer buf = ByteBuffer.allocate(size);
                    in.readFully(buf);
                    buf.flip();
                    Row row = Row.fromSpacePointer(null, 0, 0);
                    row.tableId = tableId;
                    handler.row(row);
                }
                else if (type == EntryType.COMMIT.ordinal()) {
                    long trxid = in.readLong();
                    long trxts = in.readLong();
                    handler.commit(trxid, trxts);
                }
                else if (type == EntryType.ROLLBACK.ordinal()) {
                    long trxid = in.readLong();
                    handler.rollback(trxid);
                }
                else {
                    throw new CodingError();
                }
            }
        }
        catch (EOFException x) {
        }
    }
    
    void setLogFileSize(long size) {
        this.thread.logFileSize = size;
    }

    void recover(Humpback humpback) throws Exception {
        _log.info("starting recovery process");
        
        // recover rows and transactions

        int[] counts = new int[2];
        HumpbackSession hsession = humpback.createSession();
        hsession.open();
        try {
            iterate(new WriteAheadLog.Handler() {
                Set<Integer> ignoredTables = new HashSet<Integer>();
                
                void row(Row row) {
                    int tableId = row.tableId;
                    if (this.ignoredTables.contains(tableId)) {
                        return;
                    }
                    GTable gtable = humpback.getTable(tableId);
                    if (gtable == null) {
                        _log.warn("table {} not found, all rows from recovery log for the table are ignored", tableId);
                        this.ignoredTables.add(tableId);
                        return;
                    }
                    gtable.put(hsession, row.getTrxTimestamp(), (SlowRow)null, 0);
                    if (tableId == Humpback.SYSMETA_TABLE_ID) {
                        try {
                            humpback.recoverTable();
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    counts[0]++;
                }
                
                void commit(long trxid, long trxts) {
                    humpback.commit(hsession, trxid, trxts);
                    counts[1]++;
                }
                
                void rollback(long trxid) {
                    humpback.rollback(hsession, trxid);
                }
            });
        }
        finally {
            humpback.deleteSession(hsession);
        }
        _log.info("{} rows have been recovered", counts[0]);
        _log.info("{} transactions have been recovered", counts[1]);
        
        // flush all in-memory data to storage
        
        _log.info("establishing check point");
        humpback.flush(0, true);
        
        // done
        
        _log.info("recovery is finished");
    }
}
