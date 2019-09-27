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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.nosql.ConfigService;
import com.antsdb.saltedfish.nosql.LogSpan;
import com.antsdb.saltedfish.nosql.Replicable;
import com.antsdb.saltedfish.nosql.StorageEngine;
import com.antsdb.saltedfish.nosql.StorageTable;
import com.antsdb.saltedfish.nosql.SysMetaRow;
import com.antsdb.saltedfish.util.LongLong;
import com.antsdb.saltedfish.util.UberFormatter;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author *-xguo0<@
 */
public final class MinkeCache implements LogSpan, StorageEngine {
    static final Logger _log = UberUtil.getThisLogger();
    
    /* an empty range goes from 0 to ff */
    static final Range EMPTY_RANGE = new Range(KeyBytes.getMinKey(),true, KeyBytes.getMaxKey(), true);
    
    StorageEngine stoarge;
    Minke minke;
    long cacheMiss = 0;
    ConcurrentMap<Integer, MinkeCacheTable> tableById = new ConcurrentHashMap<>();
    boolean isMutable = true;
    private int verificationMode;
    CacheStrategy strategy = new AllButBlobStrategy();
    
    public MinkeCache(StorageEngine storage) {
        this.stoarge = storage;
    }
    
    @Override
    public void open(File home, ConfigService config, boolean isMutable) throws Exception {
        this.minke = new Minke();
        this.isMutable = isMutable;
        this.strategy = config.getCacheStrategy();
        config.getProperties().setProperty("minke.size", String.valueOf(config.getCacheSize()));
        this.verificationMode = config.getCacheVerificationMode();
        this.minke.open(home, config, isMutable);
    }

    @Override
    public StorageTable getTable(int id) {
        MinkeCacheTable result = this.tableById.get(id);
        if (result == null) {
            MinkeTable mtable = (MinkeTable)this.minke.getTable(id);
            if (mtable != null) {
                _log.warn("minke and humpback are out of sync for table: {}", id);
            }
        }
        return result;
    }

    @Override
    public StorageTable createTable(SysMetaRow meta) {
        // we dont care temp. tables
        if (meta.getTableId() < 0) return null;
        
        StorageTable stable = this.stoarge.createTable(meta);
        MinkeTable mtable = (MinkeTable)this.minke.createTable(meta);
        MinkeCacheTable result = new MinkeCacheTable(this, mtable, stable, meta);
        this.tableById.put(meta.getTableId(), result);
        // set an empty range so following inserts doesn't hit hbase
        mtable.putRange(EMPTY_RANGE);
        return result;
    }

    @Override
    public boolean deleteTable(int id) {
        boolean result = true;
        if (id >= 0) {
            // only harass hbase when this is not a temporary table
            this.stoarge.deleteTable(id);
        }
        this.minke.deleteTable(id);
        this.tableById.remove(id);
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
    public void checkpoint() throws Exception {
        this.minke.checkpoint();
    }

    @Override
    public void gc(long timestamp) {
        this.minke.gc(timestamp);
    }

    public void checkpointIfNeccessary() throws Exception {
        this.minke.checkpointIfNeccessary();
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

    /**
     * is cache verification enabled ?
     * @return 0:diabled; 1:only after fetch; 2:always
     */
    int getVerificationMode() {
        return this.verificationMode;
    }

    @Override
    public void syncTable(SysMetaRow row) {
        if (row.getTableId() < 0) {
            // we dont care about temporary table
            return;
        }
        this.minke.syncTable(row);
        this.stoarge.syncTable(row);
        MinkeCacheTable mctable = this.tableById.get(row.getTableId());
        if (row.isDeleted() && mctable != null) {
            this.tableById.remove(row.getTableId());
        }
        else if (!row.isDeleted() && mctable == null) {
            MinkeTable mtable = (MinkeTable)this.minke.getTable(row.getTableId());
            StorageTable stable = this.stoarge.getTable(row.getTableId());
            mctable = new MinkeCacheTable(this, mtable, stable, row);
            this.tableById.put(row.getTableId(), mctable);
        }
    }

    @Override
    public boolean exist(int tableId) {
        return this.stoarge.exist(tableId);
    }

    @Override
    public Replicable getReplicable() {
        return this.stoarge.getReplicable();
    }
}
