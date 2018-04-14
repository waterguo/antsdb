/*-------------------------------------------------------------------------------------------------
 _______ __   _ _______ _______ ______  ______
 |_____| | \  |    |    |______ |     \ |_____]
 |     | |  \_|    |    ______| |_____/ |_____]

 Copyright (c) 2016, antsdb.com and/or its affiliates. All rights reserved. *-xguo0<@

 This program is free software: you can redistribute it and/or modify it under the terms of the
 GNU Affero General Public License, version 3, as published by the Free Software Foundation.

 You should have received a copy of the GNU Affero General Public License along with this program.
 If not, see <https://www.gnu.org/licenses/agpl-3.0.txt>
-------------------------------------------------------------------------------------------------*/
package com.antsdb.saltedfish.nosql;

import static com.antsdb.saltedfish.util.UberFormatter.hex;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.minke.MinkeCache;
import com.antsdb.saltedfish.minke.OutOfMinkeSpace;
import com.antsdb.saltedfish.util.CodingError;
import com.antsdb.saltedfish.util.FishServiceThread;
import com.antsdb.saltedfish.util.LongLong;
import com.antsdb.saltedfish.util.SizeConstants;
import com.antsdb.saltedfish.util.Speedometer;
import com.antsdb.saltedfish.util.UberFormatter;
import com.antsdb.saltedfish.util.UberTime;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * synchronize data from humpback to minke
 *  
 * @author *-xguo0<@
 */
public class Synchronizer extends FishServiceThread {
    static final Logger _log = UberUtil.getThisLogger();
    
    private Humpback humpback;
    private long tabletSp;
    private long sp;
    private HumpbackSession session;
    private long hour;
    private String state = "idling";
    private Speedometer speedometer = new Speedometer();
    private long totalOps = 0;
    private MinkeCache cache = null;
    
    Synchronizer(Humpback humpback) {
        super("synchronizer");
        this.humpback = humpback;
        this.cache = getCache();
        this.session = humpback.createSession();
        LongLong span = getStorage().getLogSpan();
        this.sp = (span == null) ? 0 : span.y;
    }
    
    private void runWhenIdle() throws Exception {
        double load = UberUtil.getSystemCpuLoad();
        if ((load >= 0) && (load <= 0.4)) {
            sync_(false);
        }
        else if (countFrozenBytes() >= SizeConstants.GB) {
            sync_(false);
        }
    }
    
    /**
     * we need a smart size so it wont fill up cache
     * 
     * @return
     */
    public long getBatchSize() {
        MinkeCache cache = getCache();
        if (cache == null) {
            return Long.MAX_VALUE;
        }
        long free = cache.getMinke().getFreeSpace();
        long result = free / 10;
        return result;
    }
    
    private boolean ifTooBig() {
        LongLong span = this.getStorage().getLogSpan();
        if (span == null) {
            return false;
        }
        long size = this.humpback.getSpaceManager().minus(this.sp, span.y);
        return size >= SizeConstants.gb(1);
    }

    private boolean ifTooLong() {
        long now = UberTime.getTime();
        now = now / 1000 / 3600;
        if (now != this.hour) {
            this.hour = now;
            return true;
        }
        return false;
    }

    private long countFrozenBytes() {
        AtomicLong result = new AtomicLong();
        TabletUtil.walkAllTablets(this.humpback, tablet -> {
            if (!tablet.isCarbonfrozen()) {
                return;
            }
            LongLong span = tablet.getLogSpan();
            if (span == null) {
                return;
            }
            result.addAndGet(tablet.getFileSize());
        });
        return result.get();
    }

    public synchronized int sync(boolean checkpoint) throws Exception {
        long opsBefore = this.totalOps;
        try {
            sync_(checkpoint);
        }
        catch (OutOfMinkeSpace x) {
        }
        long result = this.totalOps - opsBefore;
        return (int)result;
    }
    
    private synchronized int sync_(boolean checkpoint) throws Exception {
        long spReplicator = getReplicatorLogPointer();
        int result = 0;
        for (;;) {
            MemTablet tablet = TabletUtil.findOldestTablet(this.humpback, this.sp + 1, false);
            if (tablet == null) {
                break;
            }
            if (!tablet.isCarbonfrozen()) {
                break;
            }
            if (tablet.getLogSpan().y >= spReplicator) {
                // synchronizer cant go beyond replicator
                return result;
            }
             result += sync(tablet);
        }
        if (checkpoint || ifTooLong() || ifTooBig()) {
            getStorage().checkpoint();
        }
        return result;
    }
    
    public synchronized int sync(boolean checkpoint, long batchSize) throws Exception {
        if (batchSize <= 0) {
            return 0;
        }
        _log.trace("starting synchronization from {}", UberFormatter.hex(this.sp));
        boolean success = false;
        long spStart = this.sp + 1;
        AtomicInteger count = new AtomicInteger();
        boolean cacheOutOfSpace = false;
        try (HumpbackSession session=this.session.open()) {
            long spSystem = this.humpback.getSpaceManager().getAllocationPointer();
            long spActive = findActiveSp();
            long spFrozen = findFrozenSp(spActive);
            if (spFrozen == 0) {
                // nothing is frozen
                return 0;
            }
            if (spFrozen <= this.sp) {
                // nothing to sync
                return 0;
            }
            if (spFrozen >= spSystem) {
                // concurrency 
                return 0;
            }
            sync(spStart, spFrozen, count, batchSize);
            this.sp = spFrozen;
            getStorage().setEndSpacePointer(this.sp);
            if (checkpoint) {
                getStorage().checkpoint();
            }
            success = true;
            return count.get();
        }
        catch (OutOfMinkeSpace x) {
            _log.warn("cache is filled up, synchronization is partitally ended");
            cacheOutOfSpace = true;
            return count.get();
        }
        finally {
            if (count.get() != 0) {
                if (success) {
                    _log.debug(
                            "synchronization has finished with {} updates from {} to {}", 
                            count.get(), 
                            hex(spStart), 
                            hex(this.sp));
                }
                else {
                    _log.debug("synchronization has failed with {} updates from {} to {}", 
                            count.get(), 
                            hex(spStart), 
                            hex(this.sp));
                }
            }
            if (success) {
                this.state = "idling";
            }
            else {
                if (cacheOutOfSpace) {
                    this.state = "waiting for cache space";
                }
                else {
                    this.state = "failed";
                }
            }
            _log.trace("synchronization has finished");
        }
    }

    private int sync(long spStart, long spEnd, AtomicInteger count, long batchSize) throws Exception {
        SpaceManager sm = this.humpback.getSpaceManager();
        for (long i = spStart; i <= spEnd;) {
            if (Thread.interrupted()) {
                break;
            }
            long batchEnd = (batchSize == Long.MAX_VALUE) ? spEnd : sm.plus(i, batchSize, 0);
            if (batchEnd > spEnd) {
                batchEnd = spEnd;
            }
            int batchCount = 0;
            for (GTable gtable:this.humpback.getTables()) {
                batchCount += sync(gtable, i, batchEnd); 
            }
            getStorage().setEndSpacePointer(batchEnd);
            this.sp = batchEnd;
            i = batchEnd + 1;
            count.addAndGet(batchCount);
        }
        return count.get();
    }
    
    private long findFrozenSp(long activeSp) {
        AtomicLong result = new AtomicLong(0);
        TabletUtil.walkAllTablets(this.humpback, tablet -> {
            if (!tablet.isCarbonfrozen()) {
                return;
            }
            LongLong span = tablet.getLogSpan();
            if (span == null) {
                return;
            }
            if (span.y >= activeSp) {
                return;
            }
            result.set(Math.max(result.get(), span.y));
        });
        return result.get();
    }
    
    private long findActiveSp() {
        AtomicLong result = new AtomicLong(this.humpback.getSpaceManager().getAllocationPointer());
        TabletUtil.walkAllTablets(humpback, tablet -> {
            if (tablet.isCarbonfrozen()) {
                return;
            }
            LongLong span = tablet.getLogSpan();
            if (span == null) {
                return;
            }
            result.set(Math.min(result.get(), span.x));
        });
        return result.get();
    }
    
    public void syncMetadata() throws Exception {
        StorageEngine store = this.humpback.getStorageEngine();
        for (GTable i:this.humpback.getTables()) {
            StorageTable stable = store.getTable(i.getId());
            if (stable == null) {
                SysMetaRow info = this.humpback.getTableInfo(i.getId());
                store.createTable(info);
            }
        }
    }
    
    private int synchronize(long spEnd) throws IOException {
        int count = 0;
        LongLong span = humpback.getStorageEngine().getLogSpan();
        if (span.y >= spEnd) {
            return 0;
        }
        _log.info("starting synchronization from {} to {} ...", hex(span.y), hex(spEnd));
        for (GTable i:humpback.getTables()) {
            count += sync(i, span.y, spEnd);
        }
        humpback.getStorageEngine().setEndSpacePointer(spEnd);
        _log.info("synchronization has finished with {} updates", count);
        return count;
    }
    
    private int sync(GTable gtable, long spStart, long spEnd) {
        GTable source;
        StorageTable target;
        synchronized(this.humpback) {
            // table might disappear due to concurrent deletion 
            source = this.humpback.getTable(gtable.getId());
            target = getStorage().getTable(gtable.getId());
            if (source == null) {
                return 0;
            }
            if (target == null) {
                throw new CodingError();
            }
        }
        try (RowIterator iter = gtable.scanDelta(spStart, spEnd)) {
            this.state = source.toString();
            int count = ScanResultSynchronizer.synchronize(iter, target, gtable.getTableType(), () -> {
                this.totalOps ++;
                if (this.totalOps % 100 == 0) {
                    this.speedometer.sample(this.totalOps);
                }
                return null;
            });
            this.speedometer.sample(this.totalOps);
            return count;
        }
    }

    private int sync(MemTablet source) {
        // cache check
        
        if (isCacheFull()) {
            throw new OutOfMinkeSpace();
        }
        
        // check if the table is already deleted
        
        StorageTable target = getStorage().getTable(source.getTableId());
        SysMetaRow sourceTableInfo = this.humpback.getTableInfo(source.getTableId());
        TableType type = sourceTableInfo.getType();
        if (sourceTableInfo.isDeleted()) {
            // table might disappear due to concurrent deletion 
            return 0;
        }
        if (target == null) {
            throw new CodingError();
        }
        
        // start synchronization
        
        long spBackup = this.sp;
        long tabletSpBackup = this.tabletSp;
        int count = 0;
        boolean success = false;
        this.state = source.toString();
        try {
            this.session.open();
            this.state = source.toString();
            try (MemTablet.Scanner iter = source.scanDelta(this.tabletSp + 1, Long.MAX_VALUE)) {
                while((iter != null) && iter.next()) {
                    if (Thread.currentThread().isInterrupted()) {
                        break;
                    }
                    ScanResultSynchronizer.synchronizeSingleEntry(iter, target, type);
                    this.tabletSp = iter.getLogSpacePointer();
                    count++;
                    this.totalOps++;
                    if (count % 100 == 0) {
                        this.speedometer.sample(this.totalOps);
                        if (isCacheFull()) {
                            throw new OutOfMinkeSpace();
                        }
                    }
                }
                this.tabletSp = 0;
                this.speedometer.sample(this.totalOps);
                MemTablet next = TabletUtil.findOldestTablet(this.humpback, source.getLogSpan().x + 1, false);
                this.sp = (next != null) ? next.getLogSpan().x - 1 : source.getLogSpan().y + 1;
                getStorage().setEndSpacePointer(this.sp);
                success = true;
                return count;
            }
        }
        finally {
            this.session.close();
            if ((spBackup != this.sp) || (tabletSpBackup != this.tabletSp)) {
                if (success) {
                    _log.debug("{} is completeled synced with new sp={} count={}", 
                            source.toString(), 
                            hex(this.sp),
                            count);
                }
                else {
                    _log.debug("{} is partialy synced with new sp={} count={}", 
                            source.toString(), 
                            hex(this.tabletSp),
                            count);
                }
            }
        }
    }
    
    void verify(MemTablet source, TableType type, long spStart, long spEnd) {
        StorageTable target = getStorage().getTable(source.getTableId());
        try (MemTablet.Scanner iter = source.scanDelta(spStart, spEnd)) {
            while (iter.next()) {
                long pKey = iter.getKeyPointer();
                long pRow = iter.getRowPointer();
                long pResult = target.get(pKey);
                if (Row.isTombStone(pRow)) {
                    if (pResult == 0) {
                        continue;
                    }
                }
                else if (pResult != 0) {
                    continue;
                }
                _log.error("failed in verification: {}", KeyBytes.toString(pKey));
            }
        }
    }

    private StorageEngine getStorage() {
        return this.humpback.getStorageEngine();
    }
    
    private long getPendingBytes() {
        SpaceManager sm = this.humpback.getSpaceManager();
        long result = this.humpback.getSpaceManager().minus(sm.getAllocationPointer(), this.sp);
        return result;
    }
    
    public Map<String, Object> getSummary() {
        Map<String, Object> props = new HashMap<>();
        props.put("total ops", this.totalOps);
        props.put("ops/second", this.speedometer.getSpeed());
        props.put("state", this.state);
        props.put("pending data", UberFormatter.capacity(getPendingBytes()));
        props.put("log position", UberFormatter.hex(this.sp));
        props.put("committed log position", UberFormatter.hex(this.getStorage().getLogSpan().y));
        return props;
    }

    @Override
    protected boolean service() throws Exception {
        try {
            runWhenIdle();
            this.state = "idling";
            return false;
        }
        catch (OutOfMinkeSpace x) {
            this.state = "waiting for cache space";
            return false;
        }
        catch (Exception x) {
            this.state = x.getMessage();
            return false;
        }
    }
    
    private MinkeCache getCache() {
        StorageEngine storage = this.getStorage();
        return storage instanceof MinkeCache ? (MinkeCache)storage : null;
    }
    
    private boolean isCacheFull() {
        if (this.cache != null) {
            // dont use up all available space. leave 10% for stuff read from hbase
            int free = this.cache.getMinke().getFreePageCount();
            int max = this.cache.getMinke().getMaxPages();
            if (100 * free / max <= 10) {
                return true;
            }
        }
        return false;
    }
    
    private long getReplicatorLogPointer() {
        StorageEngine stor = this.getStorage();
        if (!(stor instanceof Replicable)) {
            return Long.MAX_VALUE;
        }
        long result = ((Replicable)stor).getReplicateLogPointer();
        return result;
    }
}
