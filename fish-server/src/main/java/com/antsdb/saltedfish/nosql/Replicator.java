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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.antsdb.saltedfish.nosql.Gobbler.CommitEntry;
import com.antsdb.saltedfish.nosql.Gobbler.DeleteEntry;
import com.antsdb.saltedfish.nosql.Gobbler.DeleteRowEntry;
import com.antsdb.saltedfish.nosql.Gobbler.IndexEntry;
import com.antsdb.saltedfish.nosql.Gobbler.InsertEntry;
import com.antsdb.saltedfish.nosql.Gobbler.LogEntry;
import com.antsdb.saltedfish.nosql.Gobbler.MessageEntry;
import com.antsdb.saltedfish.nosql.Gobbler.PutEntry;
import com.antsdb.saltedfish.nosql.Gobbler.RollbackEntry;
import com.antsdb.saltedfish.nosql.Gobbler.TimestampEntry;
import com.antsdb.saltedfish.nosql.Gobbler.TransactionWindowEntry;
import com.antsdb.saltedfish.nosql.Gobbler.UpdateEntry;
import com.antsdb.saltedfish.util.JumpException;
import com.antsdb.saltedfish.util.Speedometer;
import com.antsdb.saltedfish.util.UberFormatter;
import com.antsdb.saltedfish.util.UberTime;

import static com.antsdb.saltedfish.util.UberFormatter.*;

import java.io.InterruptedIOException;
import java.util.HashMap;
import java.util.Map;

import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class Replicator extends Thread {
    static final int RETRY_WAIT = 20 * 1000;
    
    private Logger log;
    private Humpback humpback;
    private Gobbler gobbler;
    private Replicable replicable;
    private ReplicationHandler replicationHandler;
    private Relay relay;
    private volatile boolean isPaused = false;
    private volatile boolean isClosed = false;
    private Exception error;
    private Speedometer speedometer = new Speedometer();
    private Stats stats = new Stats();
    private long latency;
    private String state = "";
    private int retries = 0;
    private int errors = 0;
    private TransactionReplayer trxFilter;
    private BlobReorderReplayer blobReorder;

    static class Stats {
        long inserts;
        long updates;
        long deletes;
        long trxs;
        long indexops;
        long puts;
        
        void reset() {
            this.inserts = 0;
            this.updates = 0;
            this.deletes = 0;
            this.puts = 0;
            this.indexops = 0;
            this.trxs = 0;
        }

        void merge(Stats that) {
            this.inserts += that.inserts;
            this.updates += that.updates;
            this.deletes += that.deletes;
            this.puts += that.puts;
            this.indexops += that.indexops;
            this.trxs = that.trxs;
        }

        public long getOps() {
            long result = this.inserts + this.updates + this.deletes + this.puts + this.indexops;
            return result;
        }
    }
    
    class Relay implements ReplayHandler {
        ReplayHandler downstream;
        long timestamp;
        long sp;
        int count;
        
        Relay(ReplayHandler downstream) {
            this.downstream = downstream;
        }
        
        @Override
        public void all(LogEntry entry) throws Exception {
            if (Replicator.this.isPaused || Replicator.this.isClosed) {
                throw new JumpException();
            }
            this.sp = entry.getSpacePointer();
            this.downstream.all(entry);
            if (count++ % 100 == 0) {
                Replicator.this.speedometer.sample(Replicator.this.stats.getOps());
            }
        }

        @Override
        public void insert(InsertEntry entry) throws Exception {
            this.downstream.insert(entry);
            Replicator.this.stats.inserts++;
        }

        @Override
        public void update(UpdateEntry entry) throws Exception {
            this.downstream.update(entry);
            Replicator.this.stats.updates++;
        }

        @Override
        public void put(PutEntry entry) throws Exception {
            this.downstream.put(entry);
            Replicator.this.stats.puts++;
        }

        @Override
        public void index(IndexEntry entry) throws Exception {
            this.downstream.index(entry);
            Replicator.this.stats.indexops++;
        }

        @Override
        public void deleteRow(DeleteRowEntry entry) throws Exception {
            this.downstream.deleteRow(entry);
            Replicator.this.stats.deletes++;
        }

        @Override
        public void delete(DeleteEntry entry) throws Exception {
            this.downstream.delete(entry);
            Replicator.this.stats.deletes++;
        }

        @Override
        public void commit(CommitEntry entry) throws Exception {
            this.downstream.commit(entry);
            Replicator.this.stats.trxs++;
        }

        @Override
        public void rollback(RollbackEntry entry) throws Exception {
            this.downstream.rollback(entry);
        }

        @Override
        public void message(MessageEntry entry) throws Exception {
            this.downstream.message(entry);
        }

        @Override
        public void transactionWindow(TransactionWindowEntry entry) throws Exception {
            this.downstream.transactionWindow(entry);
        }

        @Override
        public void timestamp(TimestampEntry entry) {
            this.timestamp = entry.getTimestamp();
            Replicator.this.state = "busy";
            Replicator.this.latency = UberTime.getTime() - Replicator.this.relay.timestamp;
        }
    }
    
    public Replicator(String name, Humpback humpback, Replicable replicable) {
        this.log = LoggerFactory.getLogger(UberUtil.getThisClassName() + "." + name);
        this.humpback = humpback;
        this.replicable = replicable;
        this.gobbler = this.humpback.getGobbler();
        this.replicationHandler = replicable.getReplayHandler();
        this.blobReorder = new BlobReorderReplayer(this.replicationHandler);
        this.trxFilter  = new TransactionReplayer(this.humpback.getGobbler(), this.blobReorder);
        this.relay = new Relay(trxFilter);
        setName(name);
        setDaemon(true);
    }

    @Override
    public void run() {
        log.info("{} started {} ...", getName(), this.replicable.getReplicateLogPointer());
        for (;;) {

            // sanity check
            
            if (this.isClosed) {
                this.state = "closed";
                break;
            }
            if (this.isPaused) {
                this.state = "stopped";
                UberUtil.sleep(10000);
                continue;
            }
            
            // replay the log
            
            long startSp = this.replicable.getReplicateLogPointer();
            try {
                if (this.error != null) {
                    this.retries++;
                }
                this.trxFilter.resetTransactionWindow();
                this.gobbler.replay(startSp, false, this.relay);
            }
            catch (InterruptedIOException x) {
                // expected
                continue;
            }
            catch (InterruptedException x) {
                // expected
                continue;
            }
            catch (InvalidTransactionIdException x) {
                // expected. just continue
            }
            catch (JumpException x) {
                // expected
            }
            catch (Exception x) {
                if (this.error == null) {
                    log.warn(
                            "replication failed at {}, retry later from {}",
                            hex(this.relay.sp),
                            hex(this.replicable.getReplicateLogPointer()),
                            x);
                }
                this.state = "error";
                this.error = x;
                this.errors++;
                UberUtil.sleep(RETRY_WAIT);
                continue;
            }
            
            // flush to target storage
            
            try {
                this.replicationHandler.flush();
                long endSp = this.replicable.getReplicateLogPointer();
                if (endSp != startSp) {
                    log.debug("replicated {} -> {}", hex(startSp), hex(endSp));
                    if (this.error != null) {
                        log.warn("replication resumed");
                        this.error = null;
                    }
                }
                else {
                    this.state = "idling";
                    this.latency = 0;
                    UberUtil.sleep(500);
                }
                this.error = null;
            }
            catch (InterruptedIOException x) {
                // expected
                continue;
            }
            catch (InterruptedException x) {
                // expected
                continue;
            }
            catch (Exception x) {
                if (this.error == null) {
                    log.warn("replication failed, retry later", x);
                }
                else {
                    this.retries++;
                }
                this.state = "error";
                this.error = x;
                UberUtil.sleep(RETRY_WAIT);
                continue;
            }
            // done
        }
        log.info("{} ended ...", getName());
    }

    public void pause(boolean value) {
        this.isPaused = value;
        this.interrupt();
    }
    
    public void close() {
        this.isClosed = true;
        this.interrupt();
        log.info("shutting down {} ...", getName());
        try {
            join(5000);
        }
        catch (InterruptedException e) {
        }
        log.info("{} is shut down", getName());
    }

    private long getPendingBytes() {
        SpaceManager sm = this.humpback.getSpaceManager();
        long latest = sm.getAllocationPointer();
        long current = this.replicable.getReplicateLogPointer();
        long size = sm.minus(latest, current);
        return size;
    }
    
    public Map<String, Object> getSummary() {
        Map<String, Object> props = new HashMap<>();
        props.put("total inserts", this.stats.inserts);
        props.put("total updates", this.stats.updates);
        props.put("total deletes", this.stats.deletes);
        props.put("total index ops", this.stats.indexops);
        props.put("total puts", this.stats.puts);
        props.put("total ops", this.stats.getOps());
        props.put("total transactions", this.stats.trxs);
        props.put("ops/second", this.speedometer.getSpeed());
        props.put("latency", time(this.latency));
        if (this.error == null) {
            props.put("state", this.state);
        }
        else {
            String message = "error: " + this.error.getMessage();
            props.put("state", message);
        }
        props.put("log pointer", UberFormatter.hex(this.replicable.getReplicateLogPointer()));
        props.put("errors", this.errors);
        props.put("retries", this.retries);
        props.put("pending data", capacity(getPendingBytes()));
        return props;
    }
    
    public long getLogPointer() {
        return this.replicable.getReplicateLogPointer();
    }
}
