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
package com.antsdb.saltedfish.minke;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.antsdb.saltedfish.nosql.ConfigService;
import com.antsdb.saltedfish.nosql.LogSpan;
import com.antsdb.saltedfish.nosql.Replicable;
import com.antsdb.saltedfish.nosql.ReplicationHandler;
import com.antsdb.saltedfish.nosql.StorageEngine;
import com.antsdb.saltedfish.nosql.StorageTable;
import com.antsdb.saltedfish.nosql.SysMetaRow;
import com.antsdb.saltedfish.util.LongLong;
import com.antsdb.saltedfish.util.UberFormatter;

/**
 * 
 * @author *-xguo0<@
 */
public final class MinkeCache implements Closeable, LogSpan, StorageEngine, Replicable {

    StorageEngine stoarge;
    Minke minke;
    long cacheMiss = 0;
    ConcurrentMap<Integer, MinkeCacheTable> tableById = new ConcurrentHashMap<>();
    boolean isMutable = true;
    Replicable replicable;
    private int verificationMode;
    
    public MinkeCache(StorageEngine storage) {
        this.stoarge = storage;
        if (this.stoarge.supportReplication()) {
            this.replicable = (Replicable)storage;
        }
    }
    
    @Override
    public void open(File home, ConfigService config, boolean isMutable) throws Exception {
        this.minke = new Minke();
        this.isMutable = isMutable;
        config.getProperties().setProperty("minke.size", String.valueOf(config.getCacheSize()));
        this.verificationMode = config.getCacheVerificationMode();
        this.minke.open(home, config, isMutable);
    }

    @Override
    public StorageTable getTable(int id) {
        MinkeCacheTable result = this.tableById.get(id);
        if (result == null) {
            synchronized(this) {
                MinkeTable mtable = (MinkeTable)this.minke.getTable(id);
                if (mtable != null) {
                    StorageTable stable = this.getStorage().getTable(id);
                    if (stable != null) {
                        result = new MinkeCacheTable(this, mtable, stable);
                        this.tableById.put(id, result);
                    }
                    else {
                        throw new MinkeException("minke and storage are out of sync: " + id);
                    }
                }
            }
        }
        return result;
    }

    @Override
    public StorageTable createTable(SysMetaRow meta) {
        StorageTable stable = this.stoarge.createTable(meta);
        MinkeTable mtable = (MinkeTable)this.minke.createTable(meta);
        MinkeCacheTable result = new MinkeCacheTable(this, mtable, stable);
        this.tableById.put(meta.getTableId(), result);
        return result;
    }

    @Override
    public boolean deleteTable(int id) {
        boolean result = this.stoarge.deleteTable(id);
        this.minke.deleteTable(id);
        return result;
    }

    @Override
    public void createNamespace(String name) {
        this.stoarge.createNamespace(name);
        this.minke.createNamespace(name);
    }

    @Override
    public void deleteNamespace(String name) {
        this.stoarge.deleteNamespace(name);
        this.minke.deleteNamespace(name);
    }

    @Override
    public boolean isTransactionRecoveryRequired() {
        return this.stoarge.isTransactionRecoveryRequired();
    }

    @Override
    public LongLong getLogSpan() {
        LongLong spanMinke = this.minke.getLogSpan();
        LongLong spanStorage = this.stoarge.getLogSpan();
        LongLong result = new LongLong(0, Math.min(spanMinke.y, spanStorage.y));
        return result;
    }

    @Override
    public void close() throws IOException {
        this.minke.close();
        this.stoarge.close();
    }

    void resetCacheHitRatio() {
        this.minke.resetHitCount();
        this.cacheMiss = 0;
    }

    public long getCacheHits() {
        long hits = this.minke.getHitCount();
        return hits;
    }
    
    public long getCacheMiss() {
        return this.cacheMiss;
    }
    
    public double getCacheHitRatio() {
        long hits = this.minke.getHitCount();
        long misses = this.cacheMiss;
        long total = hits + misses;
        if (total == 0) {
            return 0;
        }
        return hits/(double)total;
    }
    
    void cacheMiss() {
        this.cacheMiss++;
    }

    @Override
    public void setEndSpacePointer(long sp) {
        this.minke.setEndSpacePointer(sp);
    }

    @Override
    public synchronized void checkpoint() throws Exception {
        this.minke.checkpoint();
    }

    @Override
    public void gc(long timestamp) {
        this.minke.gc(timestamp);
    }

    public synchronized void checkpointIfNeccessary() throws Exception {
        // make a checkpoint if zombie pages take over 10% of the cache
        int zombieRatio = 100 * minke.getZombiePageCount() / minke.getMaxPages();
        if (zombieRatio >= 10) {
            checkpoint();
        }
    }
    
    public Map<String, Object> getSummary() {
        Map<String, Object> props = new HashMap<>();
        props.putAll(this.minke.getSummary());
        props.put("cache hit ratio", getCacheHitRatio());
        props.put("cache hits", getCacheHits());
        props.put("cache miss", getCacheMiss());
        props.put("max cache size", UberFormatter.capacity(this.minke.size));
        props.put("current cache file size", UberFormatter.capacity(this.minke.getCurrentFileSize()));
        props.put("cache usage %", getUsage());
        return props;
    }

    public int getUsage() {
        long used = this.minke.getUsedPageCount();
        long usage = used * 100 / this.minke.getMaxPages();
        return (int)usage;
    }
    
    void clear() {
        for (MinkeCacheTable i:this.tableById.values()) {
            MinkeTable mtable = i.mtable;
            this.minke.deleteTable(mtable.tableId);
            this.getStorage().deleteTable(mtable.tableId);
        }
        this.tableById.clear();
        resetCacheHitRatio();
    }

    public Minke getMinke() {
        return this.minke;
    }

    public StorageEngine getStorage() {
        return this.stoarge;
    }

    @Override
    public long getReplicateLogPointer() {
        if (this.replicable != null) {
            return this.replicable.getReplicateLogPointer();
        }
        else {
            return Long.MAX_VALUE;
        }
    }

    @Override
    public long getCommittedLogPointer() {
        return this.getReplicateLogPointer();
    }
    
    @Override
    public ReplicationHandler getReplayHandler() {
        return this.replicable.getReplayHandler();
    }

    @Override
    public boolean supportReplication() {
        return this.stoarge.supportReplication();
    }

    /**
     * is cache verification enabled ?
     * @return 0:diabled; 1:only after fetch; 2:always
     */
    int getVerificationMode() {
        return this.verificationMode;
    }

    @Override
    public void syncTable(SysMetaRow row) {
        this.minke.syncTable(row);
        this.stoarge.syncTable(row);
        if (row.isDeleted()) {
            return;
        }
        MinkeTable mtable = (MinkeTable)this.minke.getTable(row.getTableId());
        StorageTable stable = this.stoarge.getTable(row.getTableId());
        MinkeCacheTable mctable = new MinkeCacheTable(this, mtable, stable);
        this.tableById.put(row.getTableId(), mctable);
    }

    @Override
    public void deletes(int tableId, List<Long> deletes) {
        this.replicable.deletes(tableId, deletes);
    }

    @Override
    public void putRows(int tableId, List<Long> rows) {
        this.replicable.putRows(tableId, rows);
    }

    @Override
    public void putIndexLines(int tableId, List<Long> indexLines) {
        this.replicable.putIndexLines(tableId, indexLines);
    }

    @Override
    public boolean exist(int tableId) {
        return this.stoarge.exist(tableId);
    }
}
