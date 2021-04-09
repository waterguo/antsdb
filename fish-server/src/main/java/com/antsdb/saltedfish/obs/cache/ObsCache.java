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
package com.antsdb.saltedfish.obs.cache;

import java.io.File;
import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;

import com.antsdb.saltedfish.obs.ObsProvider;
import com.antsdb.saltedfish.parquet.ParquetUtils;
import com.antsdb.saltedfish.util.UberTime;
import com.antsdb.saltedfish.util.UberTimer;
import com.antsdb.saltedfish.util.UberUtil;
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;
import com.googlecode.concurrentlinkedhashmap.Weigher;

/**
 * 
 * @author *-xguo0<@
 */
public class ObsCache {
    static Logger _log = UberUtil.getThisLogger();

    private ConcurrentLinkedHashMap<String, ObsFileReference> map;

    public static final int DEFAULT_CONCURENCY_LEVEL = 4;

    private File localHome;
    private long capacity;
    private ObsProvider provider;
    private String remoteHome;
    private boolean isMutable;

    private AtomicLong get = new AtomicLong(0);
    private AtomicLong hit = new AtomicLong(0);

    ThreadFactory tf = new ThreadFactory() {
        public Thread newThread(Runnable r) {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setDaemon(true);
            return t;
        }
    };
    private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(tf);

    public ObsCache(long capacity, 
            ObsProvider provider, 
            File localHome, 
            String remoteHome, 
            boolean isMutable) {
        this.capacity = capacity;
        this.provider = provider;
        this.localHome = localHome;
        this.remoteHome = remoteHome;
        this.isMutable = isMutable;
        map = new ConcurrentLinkedHashMap.Builder<String, ObsFileReference>()
                .initialCapacity(0)
                .maximumWeightedCapacity(capacity)
                .weigher(ParquetFileWeigher.singleton())
                .concurrencyLevel(DEFAULT_CONCURENCY_LEVEL)
                .listener(listener).build();
        executor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                _log.trace("ObsCache status info :{}", getStatus());
            }
        }, 20, 20, TimeUnit.SECONDS);
    }

    EvictionListener<String, ObsFileReference> listener = new EvictionListener<String, ObsFileReference>() {
        @Override
        public void onEviction(String key, ObsFileReference value) {
            _log.trace("ObsCache cache Eviction capacity info:({}) ,remote key:{},last read:{}", 
                    getSumSize(), 
                    key, 
                    value.getLastRead());
        }
    };

    public ObsFileReference get(String path) throws Exception {
        if (!this.isMutable) {
            File f = File.createTempFile("tmp_" + UberTime.getTime(), ParquetUtils.DATA_PARQUET_EXT_NAME);
            try {
                String filename = f.getAbsolutePath();
                this.provider.downloadObject(this.remoteHome + path, filename);
                if (f.exists()) {
                    ObsFileReference ref = new ObsFileReference(path, f, "obs");
                    ref.setLastRead(UberTime.getTime());
                    return ref;
                }
                return null;
            }
            finally {
                f.deleteOnExit();
            }
        }
        boolean isNew = false;
        ObsFileReference syncRef = null;
        File f = new File(this.localHome, path);
        String filename = f.getAbsolutePath();
        synchronized(this) {
            syncRef = this.map.get(path);
            if (syncRef == null) {
                syncRef = new ObsFileReference(path,f,"obs");
                if (filename.endsWith(ParquetUtils.DATA_PARQUET_EXT_NAME)) {
                    this.map.put(syncRef.getKey(), syncRef);
                }
                isNew = true;
            }
        }
        UberTimer timer = new UberTimer(5*1000);
        if(isNew) {
            _log.trace("remote key:{} ,not exist by cache, download...", path);
            if( this.provider.doesObjectExist(path)) {
                this.provider.downloadObject(path, filename);
                syncRef.setDownloaded(true);
                f = new File(filename);
                if (f.exists()) {
                    _log.trace("remote key:{} not exist by cache, download success", path);
                    syncRef.setLastRead(UberTime.getTime());
                    syncRef.setFile(f);
                    if (filename.endsWith(ParquetUtils.DATA_PARQUET_EXT_NAME)) {//不需要缓存的文件直接删除
                        this.map.put(syncRef.getKey(),syncRef); 
                    }
                }
                else {
                    this.map.remove(syncRef.getKey()); 
                    _log.trace("remote key:{} not exist by cache, download fail", path);
                }
            }
            else {
                syncRef.setDownloaded(true);
                this.map.remove(syncRef.getKey()); 
                _log.trace("remote key:{} not exist, download fail", path);
                return null;
            }
        }
        else {
            for(;;) {
                if(syncRef.isDownloaded()) {
                    break;
                }
                if (timer.isExpired()) {
                    throw new TimeoutException();
                }
                try {
                    Thread.sleep(100);
                }
                catch (InterruptedException ignored) {
                }
            }
        }
        hit.addAndGet(1);
        syncRef.setLastRead(UberTime.getTime());
         
        return syncRef;
    }

    public void put(ObsFileReference ref) {
        if (!this.isMutable) {
            return;
        }
        if (ref == null || ref.getFile() == null) {
            return;
        }
        if(ref.getKey().endsWith(ParquetUtils.DATA_JSON_EXT_NAME)) {
            _log.warn("cache object is json data  key={}", ref.getKey());
            return ;
        }
        _log.trace("cache put key:{}", ref.getKey());
        ref.setLastRead(UberTime.getTime());
        ref.setDownloaded(true);
        this.map.put(ref.getKey(), ref);
    }

    public void remove(String path) {
        if (!this.isMutable) {
            return;
        }
        _log.trace("remove key:{},wait gc recyle del file...", path);
        this.map.remove(path);
    }

    public long getSumSize() {
        return this.map.weightedSize();
    }

    enum ParquetFileWeigher implements Weigher<ObsFileReference> {
        INSTANCE;

        static Logger _log = UberUtil.getThisLogger();

        @Override
        public int weightOf(ObsFileReference value) {
            try {
                int weight = 0;
                if (value == null || value.getFile() == null) {
                    _log.warn("value is empty or value's file is empty");
                    weight = 1;
                }
                else {
                    weight = (int) value.getFsize();
                }
                if(weight < 1) {
                    //_log.warn("ParquetFileWeigher is error,reset to 1,key={} val={}",value.getKey(),value.getFile());
                    weight = 1;
                }
                return weight;
            }
            catch (Exception e) {
                throw e;
            }
        }

        public static Weigher<ObsFileReference> singleton() {
            return INSTANCE;
        }
    }

    public Collection<ObsFileReference> getDatas() {
        return this.map.values();
    }

    public String getStatus() {
        StringBuilder sb = new StringBuilder();
        sb.append("current size:");
        sb.append(map.size());
        sb.append("\tsum size:");
        sb.append(this.getSumSize());
        sb.append("\tforSum size:");
        sb.append(this.getForSumSize());
        return sb.toString();
    }

    private long getForSumSize() {
        if(this.map == null || this.map.size() == 0) {
            return 0;
        }
        long sumSize = 0;
        for(ObsFileReference val :this.map.values()) {
            sumSize += val.getFsize();
        }
        return sumSize;
    }
    
    public void close() {
        if (executor != null) {
            executor.shutdown();
        }
    }

    public long getHit() {
        return hit.longValue();
    }

    public long getGetCounts() {
        return get.longValue();
    }

    public String getHitRatio() {
        StringBuilder sb = new StringBuilder();
        if (getGetCounts() > 0L) {
            sb.append(this.hit.doubleValue() / this.get.doubleValue() * 100.0D);
            sb.append("%");
        }
        else {
            sb.append("--");
        }
        return sb.toString();
    }

    public Object getUsage() {
        return this.getSumSize();
    }

    public Object getCounts() {
        return map.size();
    }

    public Object getMaxSize() {
        return this.capacity;
    }
}
