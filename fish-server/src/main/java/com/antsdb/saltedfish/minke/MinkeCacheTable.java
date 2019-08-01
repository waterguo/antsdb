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

import java.util.List;

import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.FileOffset;
import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.nosql.GetOptions;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.ScanOptions;
import com.antsdb.saltedfish.nosql.ScanResult;
import com.antsdb.saltedfish.nosql.StorageTable;
import com.antsdb.saltedfish.nosql.SysMetaRow;
import com.antsdb.saltedfish.nosql.TableType;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class MinkeCacheTable implements StorageTable {
    static final Logger _log = UberUtil.getThisLogger();
    
    MinkeTable mtable;
    StorageTable stable;
    MinkeCache cache;
    boolean cacheFull;

    MinkeCacheTable(MinkeCache minke, MinkeTable mtable, StorageTable stable, SysMetaRow meta) {
        this.mtable = mtable;
        this.stable = stable;
        this.cache = minke;
        this.cacheFull = minke.strategy.cacheFull(meta);
    }
    
    @Override
    public long get(long pKey, long options) {
        if (isFullCache()) {
            return get_full(pKey, options);
        }
        else {
            return get_partial(pKey, options); 
        }
    }
    
    private long get_full(long pKey, long options) {
        if (this.mtable.getPageCount() <= 0) {
            loadTable();
        }
        long pResult = this.mtable.get(pKey, options);
        if (pResult == 1) {
            // delete mark
            return 0;
        }
        return pResult;
    }

    private long get_partial(long pKey, long options) {
        long pResult = this.mtable.get(pKey, options);
        if (pResult == 1) {
            // delete mark
            verifyCacheGet(pKey, 0, -1, false);
            return 0;
        }
        // dont harass hbase when this is a temporary table
        if (this.getId() < 0) {
            return pResult;
        }
        if ((pResult == 0) && this.cache.isMutable) {
            pResult = this.stable.get(pKey, options);
            pResult = GetOptions.isNoCache(options) ? pResult : cache(pKey, pResult);
            if (_log.isTraceEnabled()) {
                _log.trace("fetched {} from storage count={} tableId={}", 
                        KeyBytes.toString(pKey), 
                        (pResult != 0) ? 1 : 0,
                        getId());
            }
            verifyCacheGet(pKey, -1, pResult, true);
            this.cache.cacheMiss();
        }
        else {
            verifyCacheGet(pKey, pResult, -1, false);
        }
        return pResult;
    }

    private long cache(long pKey, long pRow) {
        try {
            // the following code might throw OutofMinkeSpace exception. it is expected. we cannot use the memory
            // address from HBaseTable because it will be reset in next get() call. it is not perfect. but until
            // i find the perfect solution let's live with it
            if (pRow != 0) {
                Row row = Row.fromMemoryPointer(pRow, Row.getVersion(pRow));
                this.mtable.put(row);
                pRow = this.mtable.get(row.getKeyAddress(), false);
            }
            else {
                this.mtable.putDeleteMark(pKey);
            }
        }
        catch (OutOfMinkeSpace ignored) {
        }
        return pRow;
    }
    
    @Override
    public long getIndex(long pKey) {
        if (this.isFullCache()) {
            return getIndex_full(pKey);
        }
        else {
            return getIndex_partial(pKey);
        }
    }
    
    private long getIndex_full(long pKey) {
        if (this.mtable.getPageCount() <= 0) {
            loadTable();
        }
        return this.mtable.getIndex(pKey);
    }

    private long getIndex_partial(long pKey) {
        long pResult = this.mtable.get(pKey, 0);
        if (pResult == 1) {
            // delete mark
            verifyCacheGetIndex(pKey, 0, -1, false);
            return 0;
        }
        // dont harass hbase when this is a temporary table
        if (this.getId() < 0) {
            return pResult;
        }
        if ((pResult == 0) && this.cache.isMutable) {
            pResult = this.stable.getIndex(pKey);
            if (pResult != 0) {
                // the following code might throw OutofMinkeSpace exception. it is expected. we cannot use the memory
                // address from HBaseTable because it will be reset in next get() call. it is not perfect. but until
                // i find the perfect solution let's live with it
                pResult = this.mtable.putIndex_(pKey, pResult, (byte)0);
            }
            else {
                this.mtable.putDeleteMark(pKey);
            }
            _log.trace("fetched {} from storage count={} tableId={}", 
                    KeyBytes.toString(pKey), 
                    (pResult != 0) ? 1 : 0,
                    getId());
            verifyCacheGetIndex(pKey, -1, pResult, true);
            this.cache.cacheMiss();
        }
        else {
            verifyCacheGetIndex(pKey, pResult, -1, false);
        }
        return pResult;
    }
    
    @Override
    public ScanResult scan(long pKeyStart, long pKeyEnd, long options) {
        if (isFullCache()) {
            return scan_full(pKeyStart, pKeyEnd, options);
        }
        else {
            return scan_partial(pKeyStart, pKeyEnd, options);
        }
    }
    
    private ScanResult scan_full(long pKeyStart, long pKeyEnd, long options) {
        if (this.mtable.getPageCount() <= 0) {
            loadTable();
        }
        return this.mtable.scan(pKeyStart, pKeyEnd, options);
    }

    private ScanResult scan_partial(long pKeyStart, long pKeyEnd, long options) {
        if (getId() < 0) {
            // dont harass hbase when this is a temporary table
            return this.mtable.scan(pKeyStart, pKeyEnd, options);
        }
        else {
            boolean includeStart = ScanOptions.includeStart(options);
            boolean includeEnd = ScanOptions.includeEnd(options);
            boolean ascending = ScanOptions.isAscending(options);
            MinkeCacheTableScanner result = new MinkeCacheTableScanner(this);
            if (ScanOptions.has(options, ScanOptions.NO_CACHE)) {
                result.setCacheResult(false);
            }
            result.setRange(pKeyStart, includeStart, pKeyEnd, includeEnd, ascending);
            return result;
        }
    }

    @Override
    public synchronized void delete(long pKey) {
        if (isFullCache()) {
            if (this.mtable.getPageCount() <= 0) {
                loadTable();
            }
            this.mtable.delete(pKey);
        }
        else {
            this.mtable.putDeleteMark(pKey);
        }
    }

    @Override
    public synchronized  void putIndex(long pIndexKey, long pRowKey, byte misc) {
        if (isFullCache()) {
            if (this.mtable.getPageCount() <= 0) {
                loadTable();
            }
        }
        this.mtable.putIndex(pIndexKey, pRowKey, misc);
    }

    @Override
    public synchronized void put(Row row) {
        if (isFullCache()) {
            if (this.mtable.getPageCount() <= 0) {
                loadTable();
            }
        }
        this.mtable.put(row);
    }

    @Override
    public boolean exist(long pKey) {
        if (isFullCache()) {
            return exist_full(pKey);
        }
        else {
            return exist_partial(pKey); 
        }
    }
    
    private boolean exist_full(long pKey) {
        if (this.mtable.getPageCount() <= 0) {
            loadTable();
        }
        return this.mtable.exist(pKey);
    }

    public boolean exist_partial(long pKey) {
        long pResult = this.mtable.get(pKey, 0);
        if (pResult > 1) {
            // found it
            return true;
        }
        if (pResult == 1) {
            // delete mark
            return false;
        }
        Boundary boundary = new Boundary(pKey, BoundaryMark.NONE);
        Range range = this.mtable.findRange(boundary);
        if (range != null) {
            return false;
        }
        boolean result = false;
        ScanResult sr = this.stable.scan(pKey, KeyBytes.getMaxKey(), 0);
        range = new Range();
        try {
            int count = 0;
            if ((sr != null) && sr.next()) {
                long pFirstKey = sr.getKeyPointer();
                if (KeyBytes.compare(pKey, pFirstKey) == 0) {
                    result = true;
                    mtable.copyEntry(sr);
                    count++;
                    range.pKeyStart = pKey;
                    range.startMark = BoundaryMark.NONE;
                    if (sr.next()) {
                        mtable.copyEntry(sr);
                        count++;
                        range.pKeyEnd = sr.getKeyPointer();
                        range.endMark = BoundaryMark.NONE;
                    }
                    else {
                        range.pKeyEnd = KeyBytes.getMaxKey();
                        range.endMark = BoundaryMark.NONE;
                    }
                }
                else {
                    mtable.copyEntry(sr);
                    count++;
                    range.pKeyStart = pKey;
                    range.startMark = BoundaryMark.NONE;
                    range.pKeyEnd = pFirstKey;
                    range.endMark = BoundaryMark.NONE;
                }
            }
            else {
                // empty result
                range.pKeyStart = pKey;
                range.startMark = BoundaryMark.NONE;
                range.pKeyEnd = KeyBytes.getMaxKey();
                range.endMark = BoundaryMark.NONE;
            }
            this.mtable.putRange(range);
            _log.trace("fetched {} from storage count={} tableId={}", 
                    range.toString(), 
                    count,
                    getId());
        }
        catch (OutOfMinkeSpace x) { // ignore
        }
        finally {
            if (sr != null) {
                sr.close();
            }
        }
        this.cache.cacheMiss();
        return result;
    }
    
    private void verifyCacheGet(long pKey, long pResultFromCache, long pResultFromStorage, boolean isCacheMiss) {
        if (this.cache.getVerificationMode() < (isCacheMiss ? 1 : 2)) {
            return;
        }
        if (pResultFromStorage == -1) {
            pResultFromStorage = this.stable.get(pKey, 0); 
        }
        if (pResultFromCache == -1) {
            pResultFromCache = this.mtable.get(pKey, 0);
        }
        if (Long.compareUnsigned(pResultFromCache, 1) <= 0) {
            if (pResultFromStorage == 0) {
                return;
            }
            else {
                throw new CacheVerificationException(this.mtable.tableId, pKey, pResultFromCache, pResultFromStorage);
            }
        }
        else if (pResultFromStorage == 0) {
            throw new CacheVerificationException(this.mtable.tableId, pKey, pResultFromCache, pResultFromStorage);
        }
        if (!Row.compareByBytes(pResultFromCache, pResultFromStorage)) {
            throw new CacheVerificationException(this.mtable.tableId, pKey, pResultFromCache, pResultFromStorage);
        }
    }

    private void verifyCacheGetIndex(long pKey, long pResultFromCache, long pResultFromStorage, boolean isCacheMiss) {
        if (this.cache.getVerificationMode() < (isCacheMiss ? 1 : 2)) {
            return;
        }
        if (pResultFromStorage == -1) {
            pResultFromStorage = this.stable.get(pKey, 0); 
        }
        if (pResultFromCache == -1) {
            pResultFromCache = this.mtable.get(pKey, 0);
        }
        if (Long.compareUnsigned(pResultFromCache, 1) <= 0) {
            if (pResultFromStorage == 0) {
                return;
            }
            else {
                throw new CacheVerificationException(this.mtable.tableId, pKey, pResultFromCache, pResultFromStorage);
            }
        }
        else if (pResultFromStorage == 0) {
            throw new CacheVerificationException(this.mtable.tableId, pKey, pResultFromCache, pResultFromStorage);
        }
        if (KeyBytes.compare(pResultFromCache, pResultFromStorage) != 0) {
            throw new CacheVerificationException(this.mtable.tableId, pKey, pResultFromCache, pResultFromStorage);
        }
    }

    @Override
    public String getLocation(long pKey) {
        if (isFullCache()) {
            return getLocation_full(pKey);
        }
        else {
            return getLocation_partial(pKey);
        }
    }

    private String getLocation_full(long pKey) {
        if (this.mtable.getPageCount() <= 0) {
            loadTable();
        }
        return this.mtable.getLocation(pKey);
    }

    private String getLocation_partial(long pKey) {
        String result = this.mtable.getLocation(pKey);
        if (result != null) {
            result = this.stable.getLocation(pKey);
        }
        return result;
    }

    public int getId() {
        return this.mtable.tableId;
    }

    @Override
    public boolean traceIo(long pKey, List<FileOffset> lines) {
        return this.mtable.traceIo(pKey, lines);
    }
    
    public MinkeTable getMinkeTable() {
        return this.mtable;
    }
    
    /**
     * full cache means the entire table should be kept in minke
     * 
     * @return
     */
    public boolean isFullCache() {
        return this.cacheFull;
    }
    
    /**
     * load the entire table content from backend
     */
    private synchronized void loadTable() {
        // prevents racing condition
        if (this.mtable.getPageCount() > 0) {
            return;
        }
        
        // make sure we at least have one page
        ScanResult sr = this.stable.scan(0, 0, 0);
        this.mtable.grow(KeyBytes.getMinKey());
        
        // load table content
        long count = 0;
        while (sr.next()) {
            if (this.mtable.getType() == TableType.DATA) {
                Row row = sr.getRow();
                this.mtable.put(row);
            }
            else {
                long pIndexKey = sr.getKeyPointer();
                long pRowKey = sr.getIndexRowKeyPointer();
                byte misc = sr.getMisc();
                this.mtable.putIndex(pIndexKey, pRowKey, misc);
            }
            count++;
        }
        sr.close();
        this.cache.cacheMiss();
        _log.debug("{} records are fully cached for table {}", count, getId());
    }

    public synchronized int evictPage(MinkePage page) {
        int result = this.mtable.deletePage(page) ? 1 : 0;
        _log.debug("page {} of table {} is evicted {} {}", 
                page.id,
                getId(),
                page.copyLastAccess, 
                page.lastAccess.get());
        return result;
    }
    
    public synchronized int evictAllPages() {
        int count = 0;
        synchronized (this.mtable) {
            for (MinkePage i:this.mtable.getPages()) {
                count += evictPage(i);
            }
        }
        _log.debug("table {} is evicted", getId()); 
        return count;
    }
    
    public boolean isLoaded() {
        return this.mtable.getPageCount() > 0;
    }
}
