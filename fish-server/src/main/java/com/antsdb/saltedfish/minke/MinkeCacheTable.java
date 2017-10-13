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

import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.nosql.Row;
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
            try {
                if (pResult != 0) {
                    Row row = Row.fromMemoryPointer(pResult, Row.getVersion(pResult));
                    this.mtable.put(row);
                }
                else {
                    this.mtable.putDeleteMark(pKey);
                }
                _log.debug("fetched {} from storage count={} tableId={}", 
                        KeyBytes.toString(pKey), 
                        (pResult != 0) ? 1 : 0,
                        getId());
            }
            catch (OutOfMinkeSpace x) {
                // ignore if cache space runs out
            }
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
        throw new NotImplementedException();
    }
    
    @Override
    public ScanResult scan(long pKeyStart, boolean includeStart, long pKeyEnd, boolean includeEnd, boolean ascending) {
        MinkeCacheTableScanner result = new MinkeCacheTableScanner(this);
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
        ScanResult sr = this.stable.scan(pKey, true, KeyBytes.getMaxKey(), true, true);
        range = new Range();
        try {
            if ((sr != null) && sr.next()) {
                long pFirstKey = sr.getKeyPointer();
                if (KeyBytes.compare(pKey, pFirstKey) == 0) {
                    result = true;
                    mtable.copyEntry(sr);
                    range.pKeyStart = pKey;
                    range.startMark = BoundaryMark.NONE;
                    if (sr.next()) {
                        mtable.copyEntry(sr);
                        range.pKeyEnd = sr.getKeyPointer();
                        range.endMark = BoundaryMark.NONE;
                    }
                    else {
                        range.pKeyEnd = KeyBytes.getMaxKey();
                        range.endMark = BoundaryMark.NONE;
                    }
                }
                else {
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

    @Override
    public String getLocation(long pKey) {
        String result = this.mtable.getLocation(pKey);
        if (result != null) {
            result = this.stable.getLocation(pKey);
        }
        return result;
    }

    public Object getId() {
        return this.mtable.tableId;
    }

}
