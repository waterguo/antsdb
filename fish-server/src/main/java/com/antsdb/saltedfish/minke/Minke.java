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

import static com.antsdb.saltedfish.util.UberFormatter.hex;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.nosql.ConfigService;
import com.antsdb.saltedfish.nosql.GarbageCollector;
import com.antsdb.saltedfish.nosql.HumpbackUtil;
import com.antsdb.saltedfish.nosql.LogSpan;
import com.antsdb.saltedfish.nosql.Replicable;
import com.antsdb.saltedfish.nosql.StorageEngine;
import com.antsdb.saltedfish.nosql.StorageTable;
import com.antsdb.saltedfish.nosql.SysMetaRow;
import com.antsdb.saltedfish.nosql.TableType;
import com.antsdb.saltedfish.util.CodingError;
import com.antsdb.saltedfish.util.LongLong;
import com.antsdb.saltedfish.util.SizeConstants;
import com.antsdb.saltedfish.util.TimeConstants;
import com.antsdb.saltedfish.util.UberFormatter;
import com.antsdb.saltedfish.util.UberTime;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class Minke implements Closeable, LogSpan, StorageEngine {
    
    static Logger _log = UberUtil.getThisLogger();
    
    /** time to wait before gc the object, it is to prevent racing condition */
    static final int GC_WAIT_MS = 1000;
    
    File home;
    Set<MinkePage> garbage = new ConcurrentSkipListSet<>();
    Queue<MinkePage> free = new ConcurrentLinkedDeque<>();
    ConcurrentMap<Integer, MinkeTable> tableById = new ConcurrentHashMap<>();
    CopyOnWriteArrayList<MinkeFile> files = new CopyOnWriteArrayList<>();
    Map<Integer, MinkePage> lastFreeze = new ConcurrentHashMap<>();
    GarbageCollector gc = new GarbageCollector();
    int nextFileId = 0x100-1;
    PageIndexFile pif;
    int fileSize = 1024 * 1024 * 1024;
    int pageSize = 16 * 1024 * 1024;
    boolean isClosed = false;
    long syncSp = 0;
    long checkpointSp = 0;
    boolean isMutable = true;
    long size = Long.MAX_VALUE;
    private long nPages;
    private AtomicLong pastHits = new AtomicLong();
    private int pagesPerFile;
    private volatile int nOpenFiles;
    /** max number of files */
    private long nFiles;
    @SuppressWarnings("unused")
    private ConfigService config;
    private long lastCheckPontTime;
    private Object checkPointLock = new Object();
    
    static class GarbagePage implements Comparable<GarbagePage> {
        int child1;
        int child2;
        MinkePage page;
        long timestamp;
        
        @Override
        public int compareTo(GarbagePage that) {
            return Long.compare(this.page.id, that.page.id);
        }
    }
    
    public Minke() {
    }
    
    public Minke(File home, int fileSize, int pageSize) {
        if (pageSize >= fileSize) {
            throw new IllegalArgumentException();
        }
        this.home = home;
        this.fileSize = fileSize;
        this.pageSize = pageSize;
    }
    
    public MinkeTable createTableIfAbsent(int tableId, TableType type) {
        if (tableId < 0) {
            // we dont care about temporary table in minke
            return null;
        }
        MinkeTable table = this.tableById.get(tableId);
        if (table != null) {
            return table;
        }
        table = new MinkeTable(this, tableId, type);
        this.tableById.put(tableId, table);
        return table;
    }
    
    public MinkePage alloc(int tableId, KeyBytes startKey, KeyBytes endKey) throws IOException {
        MinkePage result = alloc(tableId);
        result.assign(tableId, startKey, endKey);
        return result;
    }
    
    public synchronized MinkePage alloc(int tableId, long pStartKey, long pEndKey) throws IOException {
        MinkePage result = alloc(tableId);
        result.assign(tableId, pStartKey, pEndKey);
        return result;
    }

    private MinkePage alloc(int tableId) throws IOException {
        MinkePage result = null;
        for (;;) {
            result = this.free.poll();
            if (result != null) {
                result.reset();
                result.lastAccess.set(UberTime.getTime());
                if (_log.isDebugEnabled()) {
                    result.callstack = new Exception();
                }
                _log.debug("page {} is allocated to table {}", hex(result.id), tableId);
                return result;
            }
            allocFileIfFull();
        }
    }
    
    private synchronized MinkeFile allocFileIfFull() throws IOException {
        if (this.free.size() > 0) {
            return null;
        }
        if ((getCurrentFileSize() + this.fileSize) > this.size) {
            throw new OutOfMinkeSpace();
        }
        int fileId = this.nextFileId;
        this.nextFileId++;
        String name = String.format("%08x.psf", fileId);
        File file = new File(this.home, name);
        MinkeFile cfile = new MinkeFile(fileId, file, this.fileSize, this.getPageSize(), isMutable);
        cfile.open();
        while (this.files.size() <= (fileId+1)) {
            this.files.add(null);
        }
        this.files.set(fileId, cfile);
        this.nOpenFiles++;
        if (this.files.get(fileId) != cfile) {
            throw new IllegalArgumentException();
        }
        for (MinkePage i:cfile.getPages()) {
            this.free.add(i);
        }
        _log.info("new minke file {} is created with {} pages", name, cfile.getPages().length);
        return cfile;
    }

    public int getPageSize() {
        return this.pageSize;
    }

    @Override
    public void open(File home, ConfigService config, boolean isMutable) throws Exception {
        this.config = config;
        if (this.files.size() != 0) {
            throw new CodingError("reopen is not supported");
        }
        if (config != null) {
            this.fileSize = config.getMinkeFileSize();
            this.pageSize = config.getMinkePageSize();
            this.size = config.getMinkeSize();
            this.nFiles = this.size / this.fileSize;
            this.pagesPerFile = (this.fileSize - MinkeFile.HEADER_SIZE) / this.pageSize;
            this.nPages = this.pagesPerFile * this.nFiles;
        }
        if (pageSize >= fileSize) {
            throw new IllegalArgumentException();
        }
        if (this.fileSize > size) {
            throw new IllegalArgumentException();
        }
        this.home = home;
        _log.info("openning minke {} ...", this.home);
        
        // create directory if necessary
        if (!this.home.exists() && isMutable) {
            _log.info("minke does not exist, creating at {} ...", this.home);
            this.home.mkdirs();
        }
        
        // open pages
        Map<Integer, MinkePage> pages = new HashMap<>();
        int maxFileId = this.nextFileId;
        for (File i:this.home.listFiles()) {
            String name = i.getName(); 
            if ((name.length() == 12) && (name.endsWith(".psf"))) {
                int fileId = Integer.parseInt(name.substring(0,  8), 16);
                maxFileId = Math.max(maxFileId, fileId);
                MinkeFile mfile = new MinkeFile(fileId, i, fileSize, pageSize, this.isMutable);
                mfile.open();
                if (this.files.size() == 0) {
                    this.fileSize = mfile.getFileSize();
                    this.pageSize = mfile.getPageSize();
                }
                while (this.files.size() <= (fileId+1)) {
                    this.files.add(null);
                }
                this.files.set(fileId, mfile);
                this.nOpenFiles++;
                for (MinkePage j:mfile.getPages()) {
                    pages.put(j.id, j);
                }
            }
        }
        this.nextFileId = maxFileId + 1;
        int nPages = pages.size();
        
        // open page index file
        this.pif = PageIndexFile.find(this.home);
        if (this.pif != null) {
            _log.info("loading pif: {}", this.pif.file);
            this.syncSp = this.pif.load(this, this.tableById);
            this.checkpointSp = this.syncSp;
        }
        _log.info("minke pif:{}", this.pif != null ? this.pif.file : null); 
        walkUsedPages((MinkePage it) -> {
            pages.remove(it.id);
            this.lastFreeze.put(it.id, it);
        });
        this.lastCheckPontTime = UberTime.getTime();
        
        // done
        this.free.addAll(pages.values());
        this.isClosed = false;
        _log.info("minke @ {} is successfully initialzied. {} pages free. {} pages allocated",
                  this.home,
                  nPages - pages.size(), 
                  pages.size());
    }
    
    @Override
    public synchronized void close() throws IOException {
        if (isClosed) {
            return;
        }
        if (this.isMutable) {
            checkpoint();
        }
        for (MinkeFile i:this.files) {
            if (i == null) {
                continue;
            }
            i.close();
        }
        this.isClosed = true;
        _log.info("minke is closed");
    }

    public void checkpoint() throws IOException {
        synchronized (this.checkPointLock) {
            if (!this.isMutable) {
                // this is impossible in read only mode
                throw new IllegalArgumentException();
            }
            
            long sp = this.syncSp;
            _log.info("creating new checkpoint with sp={}...", hex(sp));
            
            // compact the pages, free the wasted space
            Compactor compactor = new Compactor(this);
            compactor.call();
            
            // freeze page allocation
            PageIndexFile next = null;
            synchronized (this) {
                // carbonfreeze
                Map<Integer, MinkePage> newFreeze = new HashMap<>();
                for (MinkeTable mtable:this.tableById.values()) {
                    for (MinkePage i:mtable.getPages()) {
                        newFreeze.put(i.id, i);
                        if (this.lastFreeze.containsKey(i.id)) {
                            // already carbonfreezed
                            continue;
                        }
                        i.carbonfreeze();
                    }
                }
    
                // save page index file
                next = (this.pif == null) ? PageIndexFile.getFile(this.home, 0): this.pif.next();
                next.save(this.tableById, sp);
                this.checkpointSp = sp;
                this.lastCheckPontTime = UberTime.getTime();
            
                // garbage collection
                int count = 0;
                for (MinkePage i:this.lastFreeze.values()) {
                    if (!newFreeze.containsKey(i.id)) {
                        this.lastFreeze.remove(i.id);
                        freePage(i);
                        count++;
                    }
                }
                this.lastFreeze.putAll(newFreeze);
                _log.debug("{} pages have been freed", count);
            }
            
            // clean up
            PageIndexFile old = this.pif;
            this.pif = next;
            if (old != null) {
                // keep the last 3 pif files
                List<File> files = Arrays.asList(this.pif.file.getParentFile().listFiles((it)->{
                    return it.getName().endsWith(".pif");
                }));
                Collections.sort(files);
                for (int i=0; i<files.size() - 3; i++) {
                    File file = files.get(i);
                    HumpbackUtil.deleteHumpbackFile(file);
                }
            }
            
            // done
            _log.info("checkpoint {} is created", next.file);
        }
    }
    
    void clear() {
        for (MinkeTable i:this.tableById.values()) {
            i.drop();
        }
        this.tableById.clear();
        try {
            Thread.sleep(5);
        }
        catch (InterruptedException e) {
        }
        gc(UberTime.getTime());
    }

    @Override
    public LongLong getLogSpan() {
        LongLong result = new LongLong(0, this.checkpointSp);
        return result;
    }

    public MinkePage getPage(int pageId) {
        if (this.files.size() <= 0) {
            throw new IllegalArgumentException();
        }
        int fileId = pageId >>> 16;
        MinkeFile mfile = this.files.get(fileId);
        return mfile.getPage(pageId);
    }

    public void freePage(MinkePage page) {
        if (this.lastFreeze.containsKey(page.id)) {
            page.zombie();
            _log.debug("page {} is zombied with tableId={} ts={}", hex(page.id), page.tableId, page.lastAccess.get());
            return;
        }
        if (page.garbage()) {
            this.garbage.add(page);
            _log.debug("page {} is in garbage bin with tableId={} ts={}", 
                    hex(page.id), 
                    page.tableId, 
                    page.lastAccess.get());
        }
        else {
            _log.error("failed to garbage page {} state={}", hex(page.id), page.getState()); 
        }
    }

    public int getGarbagePageCount() {
        int result = this.garbage.size();
        return result;
    }
    
    public long getFreePageCount() {
        if (this.nFiles == Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        else {
            return this.free.size() + (this.nFiles - this.nOpenFiles) * this.pagesPerFile;
        }
    }
    
    private void walkUsedPages(Consumer<MinkePage> consumer) {
        for (MinkeTable mtable:this.tableById.values()) {
            for (MinkePage i:mtable.getPages()) {
                consumer.accept(i);
            }
        }
    }
    
    /**
     * garbage collection
     * 
     * @param oldestActiveQueryTimestamp
     */
    public void gc(long oldestActiveQueryTimestamp) {
        // check point to release zombie pages if it takes more than 1 gb 
        try {
            checkpointIfNeccessary();
        }
        catch (IOException x) {
            _log.error("failed to checkpoint", x);
        }
        
        // activate garbage collector
        this.gc.collect(oldestActiveQueryTimestamp);
        
        // if the garbage timestamp is older than any active query, it can be recycled
        long now = UberTime.getTime();
        List<MinkePage> list = new ArrayList<>();
        for (MinkePage i:this.garbage) {
            if (i.garbageTime >= oldestActiveQueryTimestamp) {
                // some query might be using the page
                continue;
            }
            if ((now - i.garbageTime) < GC_WAIT_MS) {
                // garbage must stay longer than 1 second before recycled. this is to prevent racing condition
                continue;
            }
            list.add(i);
        }
        
        // recycle
        synchronized (this) {
            int count = 0;
            for (MinkePage page:list) {
                this.garbage.remove(page);
                if (page.free()) {
                    _log.debug("page {} is freed", hex(page.id));
                    this.pastHits.addAndGet(page.hit.get());
                    this.free.add(page);
                    count++;
                }
                else {
                    this.garbage.add(page);
                }
            }
            if (count != 0) {
                _log.debug("{} pages have been recycled", count);
            }
        }
    }
    
    void waitForGcReady() {
        for (long start = UberTime.getTime(); UberTime.getTime() - start < GC_WAIT_MS;) {
            UberUtil.sleep(100);
        }
    }
    
    void checkpointIfNeccessary() throws IOException {
        synchronized (this.checkPointLock) {
            // make a checkpoint if zombie pages take over 1GB
            if (this.pageSize * getZombiePageCount() >= SizeConstants.GB) {
                checkpoint();
                return;
            }
            
            // make a checkpoint if log space is more than 1gb
            /* temporarily disabled. the following logic could trigger checkpoint() endlessly
            long nfiles = this.syncSp >> 32 - this.checkpointSp >> 32;
            long size = this.config.getSpaceFileSize() * nfiles;
            if (size >= SizeConstants.GB) {
                checkpoint();
                return;
            }
            */
            
            // make a checkpoint if last checkpoint is older than 1 hour
            if (UberTime.getTime() - this.lastCheckPontTime >= TimeConstants.hour(1)) {
                checkpoint();
                return;
            }
        }
    }

    void validate() {
        for (MinkeTable i:this.tableById.values()) {
            i.validate();
        }
    }

    @Override
    public StorageTable getTable(int tableId) {
        MinkeTable table = this.tableById.get(tableId);
        return table;
    }

    @Override
    public StorageTable createTable(SysMetaRow meta) {
        if (meta.getTableId() < 0) {
            // we dont care about temporary table in minke
            return null;
        }
        int tableId = meta.getTableId();
        TableType type = meta.getType();
        MinkeTable table = this.tableById.get(tableId);
        if (table != null) {
            return table;
        }
        table = new MinkeTable(this, tableId, type);
        this.tableById.put(tableId, table);
        return table;
    }

    @Override
    public boolean deleteTable(int tableId) {
        MinkeTable table = this.tableById.get(tableId);
        if (table == null) {
            return false;
        }
        this.tableById.remove(tableId);
        table.deleteAllPages();
        return true;
    }

    @Override
    public void createNamespace(String name) {
    }

    @Override
    public void deleteNamespace(String name) {
    }

    @Override
    public boolean isTransactionRecoveryRequired() {
        return false;
    }

    public long getHitCount() {
        long count = 0;
        for (MinkeTable mtable:this.tableById.values()) {
            for (MinkePage i:mtable.getPages()) {
                count += i.hit.get();
            }
        }
        return count + this.pastHits.get();
    }
    
    void resetHitCount() {
        for (MinkeTable mtable:this.tableById.values()) {
            for (MinkePage i:mtable.getPages()) {
                i.hit.set(0);
            }
        }
        this.pastHits.set(0);
    }

    @Override
    public void setEndSpacePointer(long sp) {
        this.syncSp = sp;
    }

    public int getCurrentPageCount() {
        int count = 0;
        for (MinkeFile i:this.files) {
            if (i == null) {
                continue;
            }
            count += i.getPages().length;
        }
        return count;
    }
    
    public long getMaxPages() {
        return this.nPages;
    }

    public Map<String, Object> getSummary() {
        int fileCount = 0;
        for (MinkeFile i:this.files) {
            if (i != null) {
                fileCount++;
            }
        }
        Map<String, Object> props = new HashMap<>();
        if (getLogSpan() != null) {
            LongLong span = getLogSpan();
            props.put("log span", UberFormatter.hex(span.x) + "," + UberFormatter.hex(span.y));
        }
        else {
            props.put("log span", "");
        }
        props.put("number of files", fileCount);
        props.put("number of opened pages", getCurrentPageCount());
        props.put("number of max. pages", getMaxPages());
        props.put("page size", getPageSize());
        props.put("number of free pages", getFreePageCount());
        props.put("number of garbage pages", getGarbagePageCount());
        props.put("number of used pages", getUsedPageCount());
        props.put("number of zombie pages", getZombiePageCount());
        props.put("file size", getCurrentFileSize());
        // props.put("missing pages", StringUtils.left(findDisappearedPages(), 50));
        return props;
    }
    
    public int getZombiePageCount() {
        int result = getCurrentPageCount() - getUsedPageCount() - getGarbagePageCount() - this.free.size();
        return result;
    }
    
    long getCurrentFileSize() {
        long result = 0;
        for (MinkeFile file:this.files) {
            if (file == null) {
                continue;
            }
            result += this.fileSize;
        }
        return result;
    }
    
    int getUsedPageCount() {
        int count = 0;
        for (MinkeTable i:this.tableById.values()) {
            count += i.getPageCount();
        }
        return count;
    }

    public long getFreeSpace() {
        long size = getFreePageCount() * this.pageSize;
        return size;
    }
    
    String findDisappearedPages() {
        Set<Integer> pages = new HashSet<>();
        for (MinkeTable i:this.tableById.values()) {
            for (MinkePage j:i.getPages()) {
                pages.add(j.id);
            }
        }
        for (MinkePage i:this.lastFreeze.values()) {
            pages.add(i.id);
        }
        for (MinkePage i:this.free) {
            pages.add(i.id);
        }
        for (MinkePage i:this.garbage) {
            pages.add(i.id);
        }
        List<String> missing = new ArrayList<>();
        for (MinkeFile i:this.files) {
            if (i == null) {
                continue;
            }
            for (MinkePage j:i.getPages()) {
                if (!pages.contains(j.id)) {
                    _log.debug("", j.callstack);
                    missing.add(hex(j.id));
                }
            }
        }
        return StringUtils.join(missing, ',');
    }

    @Override
    public void syncTable(SysMetaRow meta) {
        if (meta.getTableId() < 0) {
            // we dont care about temporary table in minke
            return;
        }
        MinkeTable mtable = this.tableById.get(meta.getTableId());
        if (meta.isDeleted() && mtable != null) {
            deleteTable(mtable.tableId);
        }
        else if (!meta.isDeleted() && mtable == null) {
            createTable(meta);
        }
    }

    @Override
    public boolean exist(int tableId) {
        return this.tableById.get(tableId) != null;
    }
    
    public List<MinkeFile> getFiles() {
        return this.files;
    }

    @Override
    public Replicable getReplicable() {
        return null;
    }
    
    public Collection<MinkeTable> getTables() {
        return Collections.unmodifiableCollection(this.tableById.values());
    }
}
