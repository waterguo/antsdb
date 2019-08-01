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
package com.antsdb.saltedfish.sql.vdm;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.minke.MinkeCache;
import com.antsdb.saltedfish.minke.MinkeCacheTable;
import com.antsdb.saltedfish.minke.Range;
import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.RowIterator;
import com.antsdb.saltedfish.nosql.ScanOptions;
import com.antsdb.saltedfish.nosql.ScanResult;
import com.antsdb.saltedfish.nosql.StorageEngine;
import com.antsdb.saltedfish.nosql.StorageTable;
import com.antsdb.saltedfish.nosql.TableType;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.util.CursorUtil;
import com.antsdb.saltedfish.util.Differ;
import com.antsdb.saltedfish.util.UberUtil;

import static com.antsdb.saltedfish.minke.BoundaryMark.*;

/**
 * 
 * @author *-xguo0<@
 */
public class CompareCache extends View {
    static Logger _log = UberUtil.getThisLogger();
    
    int tableId = -1;
    boolean showAll = false;
    private boolean compareFullyCached = true;
    private boolean comparePartiallyCached = true;
    
    private static class HumpbackRowSupperlier implements Iterator<Long> {
        private RowIterator upstream;
        private boolean hasNext = true;

        HumpbackRowSupperlier(RowIterator upstream) {
            this.upstream = upstream;
        }
        
        @Override
        public boolean hasNext() {
            return this.hasNext;
        }

        @Override
        public Long next() {
            if (!this.hasNext ) {
                return null;
            }
            if (!this.upstream.next()) {
                this.hasNext = false;
                return KeyBytes.getMaxKey();
            }
            return this.upstream.getKeyPointer();
        }
    }
    
    private static class StorageRowSupperlier implements Iterator<Long> {
        private ScanResult upstream;
        private boolean hasNext = true;

        StorageRowSupperlier(ScanResult upstream) {
            this.upstream = upstream;
        }
        
        @Override
        public boolean hasNext() {
            return this.hasNext;
        }

        @Override
        public Long next() {
            if (!this.hasNext ) {
                return null;
            }
            if (!this.upstream.next()) {
                this.hasNext = false;
                return KeyBytes.getMaxKey();
            }
            return this.upstream.getKeyPointer();
        }
    }
    
    private static class MyComparator implements Comparator<Long> {
        @Override
        public int compare(Long x, Long y) {
            return KeyBytes.compare(x, y);
        }
    }
    
    public static class Line {
        public long TABLE_ID;
        public String TABLE_TYPE;
        public String CACHE_MODE;
        public long ROWS_IN_CACHE;
        public long ROWS_IN_STORAGE;
        public long ROWS_DIFFERENT;
        public String comment;
        long rowsCompared;
    }
    
    public CompareCache() {
        super(CursorUtil.toMeta(Line.class));
    }

    public CompareCache(int tableId) {
        super(CursorUtil.toMeta(Line.class));
        this.tableId = tableId;
    }

    public void setOptions(Properties props) {
        for (Map.Entry<Object, Object> i:props.entrySet()) {
            String key = i.getKey().toString();
            String value = i.getValue().toString();
            if (key.equalsIgnoreCase("result")) {
                if (value.equalsIgnoreCase("all")) {
                    this.showAll = true;
                }
                else if (value.equalsIgnoreCase("error")) {
                    this.showAll = false;
                }
                else {
                    throw new OrcaException("invalid value for option 'result': {}", value); 
                }
            }
            else if (key.equalsIgnoreCase("category")) {
                if (value.equalsIgnoreCase("all")) {
                    this.compareFullyCached = true;
                    this.comparePartiallyCached = true;
                }
                else if (value.equalsIgnoreCase("full")) {
                    this.compareFullyCached = true;
                    this.comparePartiallyCached = false;
                }
                else if (value.equalsIgnoreCase("partial")) {
                    this.compareFullyCached = false;
                    this.comparePartiallyCached = true;
                }
                else {
                    throw new OrcaException("invalid value for option 'category': {}", value); 
                }
            }
            else {
                throw new OrcaException("invalid option: {}", key); 
            }
        }
    }
    
    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        Humpback humpback = ctx.getHumpback();
        StorageEngine stor = humpback.getStorageEngine();
        if (!(stor instanceof MinkeCache)) {
            throw new OrcaException("cache is not enabled");
        }
        MinkeCache cache = (MinkeCache) stor;
        stor = cache.getStorage();
        List<Line> result = new ArrayList<>();
        if (this.tableId != -1) {
            compareSingle(humpback, cache, stor, result);
        }
        else {
            compareAll(humpback, cache, stor, result);
        }
        return CursorUtil.toCursor(this.meta, result);
    }

    private void compareAll(Humpback humpback, MinkeCache cache, StorageEngine stor, List<Line> result) {
        int ntables = 0;
        long nrows = 0;
        long ndifference = 0;
        for (GTable i:humpback.getTables()) {
            if (i.getId() < 0) {
                continue;
            }
            MinkeCacheTable mctable = (MinkeCacheTable) cache.getTable(i.getId());
            if (mctable.isFullCache() && !this.compareFullyCached) {
                continue;
            }
            if (!mctable.isFullCache() && !this.comparePartiallyCached) {
                continue;
            }
            Line line = compare(humpback, mctable, stor);
            if (this.showAll) {
                result.add(line);
            }
            else if (line.ROWS_DIFFERENT != 0) {
                result.add(line);
            }
            ntables++;
            ndifference += line.ROWS_DIFFERENT;
            nrows += line.rowsCompared;
        }
        _log.debug("{} tables compared, {} rows compared, {} difference found", ntables, nrows, ndifference);
    }

    private void compareSingle(Humpback humpback, MinkeCache cache, StorageEngine stor, List<Line> result) {
        MinkeCacheTable mctable = (MinkeCacheTable)cache.getTable(this.tableId);
        if (mctable == null) {
            throw new OrcaException("table {} is not found", this.tableId); 
        }
        result.add(compare(humpback, mctable, stor));
    }

    private Line compare(Humpback humpback, MinkeCacheTable mctable, StorageEngine stor) {
        Line result = new Line();
        result.TABLE_ID = mctable.getId();
        StorageTable stable = stor.getTable(mctable.getId());
        if (!mctable.isLoaded()) {
            result.comment = "table is not cached";
        }
        else if (stable == null) {
            result.comment = "storage table is not found";
        } 
        else {
            _log.debug("comparing {} ...", mctable.getId());
            synchronized(mctable) {
                GTable gtable = humpback.getTable(mctable.getId());
                result.TABLE_TYPE = String.valueOf(gtable.getTableType());
                result.CACHE_MODE = mctable.isFullCache() ? "full" : "partial";
                if (mctable.isFullCache()) {
                    compareFullCached(result, mctable, gtable, stable);
                }
                else if (this.comparePartiallyCached) {
                    comparePartialCached(result, mctable, gtable, stable);
                }
                else {
                    result.comment = "excluded";
                }
            }
            _log.debug("comparison of {} is completed rows_in_cache={} rows_in_stor={} rows_diff={}", 
                    mctable.getId(),
                    result.ROWS_IN_CACHE,
                    result.ROWS_IN_STORAGE,
                    result.ROWS_DIFFERENT);
        }
        return result;
    }
    
    private void compareFullCached(Line result, MinkeCacheTable mctable, GTable gtable, StorageTable stable) {
        compareRange(result, gtable, stable, 0, 0, 0);
    }

    private void comparePartialCached(Line result, MinkeCacheTable mctable, GTable gtable, StorageTable stable) {
        Supplier<Range> i=mctable.getMinkeTable().scanRanges(true);
        boolean isIndex = gtable.getTableType() != TableType.DATA;
        for (;;) {
            Range ii = i.get();
            if (ii == null) break;
            if (ii.isDot()) {
                long pKey = ii.getStart().getKey();
                long pData = isIndex ? stable.getIndex(pKey) : stable.get(pKey, 0);
                if (pData != 0) {
                    result.ROWS_IN_CACHE++;
                    result.ROWS_IN_STORAGE++;
                    result.rowsCompared++;
                }
                else {
                    result.ROWS_IN_STORAGE++;
                    result.ROWS_DIFFERENT++;
                    result.rowsCompared++;
                }
            }
            else {
                long options = 0;
                options = ii.getStartMark()==PLUS ? ScanOptions.excludeStart(options) : options;
                options = ii.getEndMark()==MINUS ? ScanOptions.excludeEnd(options) : options;
                compareRange(result, gtable, stable, ii.getStartKey(), ii.getEndKey(), options);
            }
        }
    }

    private void compareRange(Line result, 
                              GTable gtable, 
                              StorageTable stable, 
                              long pStartKey, 
                              long pEndKey, 
                              long options) {
        RowIterator scanx = gtable.scan(0, Long.MAX_VALUE, pStartKey, pEndKey, options);
        ScanResult scany = stable.scan(pStartKey, pEndKey, options);
        Differ<Long> differ = new Differ<>(
                new HumpbackRowSupperlier(scanx), 
                new StorageRowSupperlier(scany), 
                new MyComparator());
        differ.diff((state, x,y) -> {
            if (state == Differ.State.ADD) {
                result.ROWS_IN_STORAGE++;
                result.ROWS_DIFFERENT++;
                result.rowsCompared++;
            }
            else if (state == Differ.State.DELETE) {
                result.ROWS_IN_CACHE++;
                result.ROWS_DIFFERENT++;
                result.rowsCompared++;
            }
            else {
                result.ROWS_IN_CACHE++;
                result.ROWS_IN_STORAGE++;
                result.rowsCompared++;
            }
        });
        scanx.close();
        scany.close();
        result.ROWS_IN_CACHE--;
        result.ROWS_IN_STORAGE--;
    }
}
