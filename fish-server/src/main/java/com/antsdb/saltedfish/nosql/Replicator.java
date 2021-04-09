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

import static com.antsdb.saltedfish.util.UberFormatter.capacity;
import static com.antsdb.saltedfish.util.UberFormatter.hex;
import static com.antsdb.saltedfish.util.UberFormatter.time;

import java.io.InterruptedIOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;

import com.antsdb.saltedfish.util.JumpException;
import com.antsdb.saltedfish.util.Speedometer;
import com.antsdb.saltedfish.util.UberFormatter;
import com.antsdb.saltedfish.util.UberTime;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class Replicator<E extends Replicable> extends Thread {
    static final Logger _log = UberUtil.getThisLogger();
    static final int RETRY_WAIT = 30 * 1000;
    
    private Humpback humpback;
    private Gobbler gobbler;
    private E replicable;
    private volatile boolean isPaused = false;
    private volatile boolean isClosed = false;
    private Exception error;
    private Speedometer speedometer = new Speedometer();
    private Stats stats = new Stats();
    private long latency;
    private int retries = 0;
    private int errors = 0;
    private boolean hasOpened;
    private Wrapper wrapper = null;
    private boolean isIdling = false;
    private boolean isConnected = false;
    private boolean isStarted = false;
    private Knob knob;

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
    
    class Relay implements ReplayHandler, ReplicationHandler2 {
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
        public void insert(InsertEntry2 entry) throws Exception {
            if (entry.getTableId() < 0) {
                return;
            }
            this.downstream.insert(entry);
            Replicator.this.stats.inserts++;
        }

        @Override
        public void update(UpdateEntry2 entry) throws Exception {
            if (entry.getTableId() < 0) {
                return;
            }
            this.downstream.update(entry);
            Replicator.this.stats.updates++;
        }

        @Override
        public void put(PutEntry2 entry) throws Exception {
            if (entry.getTableId() < 0) {
                return;
            }
            this.downstream.put(entry);
            Replicator.this.stats.puts++;
        }

        @Override
        public void index(IndexEntry2 entry) throws Exception {
            if (entry.getTableId() < 0) {
                return;
            }
            this.downstream.index(entry);
            Replicator.this.stats.indexops++;
        }

        @Override
        public void deleteRow(DeleteRowEntry2 entry) throws Exception {
            if (entry.getTableId() < 0) {
                return;
            }
            this.downstream.deleteRow(entry);
            Replicator.this.stats.deletes++;
        }

        @Override
        public void delete(DeleteEntry2 entry) throws Exception {
            if (entry.getTableId() < 0) {
                return;
            }
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
        public void message(MessageEntry2 entry) throws Exception {
            this.downstream.message(entry);
        }

        @Override
        public void transactionWindow(TransactionWindowEntry entry) throws Exception {
            this.downstream.transactionWindow(entry);
        }

        @Override
        public void timestamp(TimestampEntry entry) throws Exception {
            this.timestamp = entry.getTimestamp();
            Replicator.this.latency = UberTime.getTime() - this.timestamp;
            Replicator.this.knob.pong();
            Replicator.this.knob.setProgress("lp:" + entry.sp);
        }

        @Override
        public void ddl(DdlEntry entry) throws Exception {
            this.downstream.ddl(entry);
        }
    }
    
    private abstract class Wrapper {
        abstract long replay(long lpStart) throws Exception;
        abstract long getCurrentLogPointer();
        abstract long getCommitedLogPointer();
        abstract void flush(long stopSp) throws Exception;
    }
    
    private class SequentialWrapper extends Wrapper {
        private ReplicationHandler2 replicationHandler;
        private BlobReorderReplayer blobReorder;
        private TransactionReplayer trxFilter;
        private Relay relay;
        private SequentialLogReader reader = new SequentialLogReader(Replicator.this.gobbler);

        public SequentialWrapper(ReplicationHandler2 handler, boolean needOrderedBlob) {
            this.replicationHandler = handler;
            ReplicationHandler2 temp = this.replicationHandler;
            if (needOrderedBlob) {
                this.blobReorder = new BlobReorderReplayer(temp);
                temp = this.blobReorder;
            }
            this.trxFilter = new TransactionReplayer(Replicator.this.humpback.getGobbler(), temp);
            this.relay = new Relay(this.trxFilter);
        }
        
        @Override
        long replay(long lpStart) throws Exception {
            this.trxFilter.resetTransactionWindow();
            this.reader.setPosition(lpStart, false);
            this.reader.replay(this.relay);
            return this.reader.getLast();
        }

        @Override
        long getCurrentLogPointer() {
            long result = Replicator.this.replicable.getReplicateLogPointer();
            if (this.blobReorder != null) {
                result = Math.min(result, this.blobReorder.getLogPointer());
            }
            return result;
        }

        @Override
        long getCommitedLogPointer() {
            long result = Replicator.this.replicable.getReplicateLogPointer();
            if (this.blobReorder != null) {
                result = Math.min(result, this.blobReorder.getLogPointer());
            }
            return result;
        }

        @Override
        void flush(long stopSp) throws Exception {
            this.replicationHandler.flush(stopSp, 0);
        }
    }
    
    private class OrderedWrapper extends Wrapper {
        private ReplicationHandler2 replicationHandler2;
        private OrderedLogReader logReader;
        
        OrderedWrapper(ReplicationHandler2 handler, long batchSize) {
            this.replicationHandler2 = handler;
            this.logReader = new OrderedLogReader(humpback,batchSize);
        }

        @Override
        long replay(long lpStart) throws Exception {
            // don't include the start 
            // this.logReader.setPosition(lpStart + 1);
            this.logReader.setPosition(lpStart);
            this.logReader.replay(this.replicationHandler2);
            return this.logReader.getPosition();
        }

        @Override
        long getCurrentLogPointer() {
            return this.logReader.getPosition();
        }

        @Override
        long getCommitedLogPointer() {
            long result = Replicator.this.replicable.getReplicateLogPointer();
            return result;
        }

        @Override
        void flush(long stopSp) throws Exception {
            this.replicationHandler2.flush(stopSp, 0);
        }
    }
    
    /**
     * 
     * @param name
     * @param humpback
     * @param replicable
     * @param orderBlob if blob rows are ordered to their master
     * @param ordered use OrderedLogReader or not
     */
    public Replicator(String name, Humpback humpback, E replicable, boolean orderBlob, boolean ordered) {
        this.humpback = humpback;
        this.replicable = replicable;
        this.gobbler = this.humpback.getGobbler();
        ReplicationHandler2 handler = replicable.getReplayHandler();
        if (ordered) {
            this.wrapper = new OrderedWrapper((ReplicationHandler2)handler, humpback.getConfig().getSyncBatchSize());
        }
        else if (handler instanceof ReplicationHandler2) {
            this.wrapper = new SequentialWrapper((ReplicationHandler2)handler, orderBlob);
        }
        this.knob = humpback.getScheduler().createKnob(name, 0);
        setName(name + "-" + getId());
        setDaemon(true);
    }

    @Override
    public void run() {
        this.knob.setThread(Thread.currentThread());
        for (;;) {
            // sanity check
            if (this.isClosed) {
                this.isStarted  = false;
                break;
            }
            if (this.isPaused) {
                if (this.isStarted) {
                    _log.info("stopped");
                    this.isStarted  = false;
                }
                try {
                    Thread.sleep(10000);
                }
                catch (InterruptedException e) {}
                continue;
            }
            if (!this.isStarted) {
                _log.info("started");
                this.isStarted = true;
            }
            
            try {
                run0();
                if (this.error != null) {
                    this.error = null;
                }
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
                _log.error(x.getMessage(),x);
                if (this.error == null) {
                    _log.info(
                            "replication failed at {}, retry later from {}",
                            hex(this.wrapper.getCurrentLogPointer()),
                            hex(this.replicable.getReplicateLogPointer()),
                            x);
                }
                this.error = x;
                this.errors++;
                UberUtil.sleep(RETRY_WAIT);
                continue;
            }
        }
        _log.info("ended");
    }

    private void run0() throws Exception {
        // replay the log
        long startSp = -1;
        long stopSp = 0;
        try {
            // open
            connect();
            
            // replay
            startSp = this.replicable.getReplicateLogPointer();
            if (this.error != null) {
                this.retries++;
            }
            stopSp = this.wrapper.replay(startSp);
        }
        catch (InvalidTransactionIdException x) {
            // expected. just continue
            _log.trace("InvalidTransactionIdException");
        }
        catch (JumpException x) {
            // expected
        }
        
        // flush to the storage, we still want to do a partial flush even if replay has errors
        this.wrapper.flush(stopSp);
        long endSp = this.replicable.getReplicateLogPointer();
        if (endSp != startSp) {
            _log.debug("replicated {} -> {}", hex(startSp), hex(endSp));
            if (this.error != null) {
                _log.info("replication resumed");
                this.error = null;
            }
        }
        else {
            idle();
        }
        this.error = null;
    }
    
    private void connect() throws Exception {
        boolean succeed = false;
        try {
            this.replicable.connect();
            if (!this.hasOpened) {
                this.hasOpened = true;
                _log.info("start replicating from 0x{}", 
                          Long.toHexString(this.replicable.getReplicateLogPointer())); 
            }
            succeed = true;
        }
        finally {
            this.isConnected = succeed;
        }
    }

    private void idle() {
        try {
            this.isIdling  = true;
            this.latency = 0;
            UberUtil.sleep(500);
        }
        finally {
            this.isIdling = false;
        }
    }

    public void pause(boolean value) {
        this.isPaused = value;
        if (value) {
            this.interrupt();
        }
    }
    
    public void close() {
        this.isClosed = true;
        this.interrupt();
        _log.info("shutting down {} ...", getName());
        try {
            join(5000);
        }
        catch (InterruptedException e) {
        }
        _log.info("{} is shut down", getName());
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
            props.put("state", getReplicatorState());
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
    
    private Object getReplicatorState() {
        if (!this.isStarted) {
            return "stopped";
        }
        if (!this.isConnected) {
            return "disconnected";
        }
        if (this.error != null) {
            return "error";
        }
        if (this.isIdling) {
            return "idling";
        }
        return "busy";
    }

    public long getLogPointer() {
        return this.wrapper.getCommitedLogPointer();
    }
    
    public E getReplicable() {
        return this.replicable;
    }
    
    public Exception getError() {
        return this.error;
    }
}
