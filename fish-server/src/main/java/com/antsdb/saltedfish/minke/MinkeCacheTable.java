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
package com.antsdb.saltedfish.minke;

import java.util.List;

import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.FileOffset;
import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.ScanOptions;
import com.antsdb.saltedfish.nosql.ScanResult;
import com.antsdb.saltedfish.nosql.StorageTable;
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

    MinkeCacheTable(MinkeCache minke, MinkeTable mtable, StorageTable stable) {
        this.mtable = mtable;
        this.stable = stable;
        this.cache = minke;
    }
    
    @Override
    public long get(long pKey) {
        long pResult = this.mtable.get(pKey);
        if (pResult == 1) {
            // delete mark
            verifyCacheGet(pKey, 0, -1, false);
            return 0;
        }
        if ((pResult == 0) && this.cache.isMutable) {
            pResult = this.stable.get(pKey);
            if (pResult != 0) {
                // the following code might throw OutofMinkeSpace exception. it is expected. we cannot use the memory
                // address from HBaseTable because it will be reset in next get() call. it is not perfect. but until
                // i find the perfect solution let's live with it
                Row row = Row.fromMemoryPointer(pResult, Row.getVersion(pResult));
                pResult = this.mtable.put_(row);
            }
            else {
                this.mtable.putDeleteMark(pKey);
            }
            _log.debug("fetched {} from storage count={} tableId={}", 
                    KeyBytes.toString(pKey), 
                    (pResult != 0) ? 1 : 0,
                    getId());
            verifyCacheGet(pKey, -1, pResult, true);
            this.cache.cacheMiss();
        }
        else {
            verifyCacheGet(pKey, pResult, -1, false);
        }
        return pResult;
    }

    @Override
    public long getIndex(long pKey) {
        long pResult = this.mtable.get(pKey);
        if (pResult == 1) {
            // delete mark
            verifyCacheGetIndex(pKey, 0, -1, false);
            return 0;
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
            _log.debug("fetched {} from storage count={} tableId={}", 
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

    @Override
    public void delete(long pKey) {
        this.mtable.putDeleteMark(pKey);
    }

    @Override
    public void putIndex(long pIndexKey, long pRowKey, byte misc) {
        this.mtable.putIndex(pIndexKey, pRowKey, misc);
    }

    @Override
    public void put(Row row) {
        this.mtable.put(row);
    }

    @Override
    public boolean exist(long pKey) {
        long pResult = this.mtable.get(pKey);
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
            _log.debug("fetched {} from storage count={} tableId={}", 
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
            pResultFromStorage = this.stable.get(pKey); 
        }
        if (pResultFromCache == -1) {
            pResultFromCache = this.mtable.get(pKey);
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
            pResultFromStorage = this.stable.get(pKey); 
        }
        if (pResultFromCache == -1) {
            pResultFromCache = this.mtable.get(pKey);
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
}
