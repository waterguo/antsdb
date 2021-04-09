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
import java.util.concurrent.atomic.AtomicLong;
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
import com.antsdb.saltedfish.util.JumpException;
import com.antsdb.saltedfish.util.UberUtil;

import static com.antsdb.saltedfish.minke.BoundaryMark.*;

/**
 * 
 * @author *-xguo0<@
 */
public class CompareCache extends View {
    static Logger _log = UberUtil.getThisLogger();
    
    int tableId = -1;
    /** show the diff result, even the ones found no error */
    boolean showAll = false;
    /** compare fully cached tables */
    private boolean compareFullyCached = true;
    /** compare partially cached tables */
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
        public String KEY;
        public int IN_CACHE;
        public int IN_STORAGE;
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
            if (key.equalsIgnoreCase("category")) {
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
        try {
            if (this.tableId != -1) {
                compareSingle(humpback, cache, stor, result);
            }
            else {
                compareAll(humpback, cache, stor, result);
            }
        }
        catch (JumpException x) {
            // too much result
            Line line = new Line();
            line.KEY = "too many differences";
            result.add(line);
        }
        return CursorUtil.toCursor(this.meta, result);
    }

    private void compareAll(Humpback humpback, MinkeCache cache, StorageEngine stor, List<Line> result) {
        int ntables = 0;
        long nrows = 0;
        List<GTable> tables = new ArrayList<>(humpback.getTables());
        tables.sort((x,y)->{return Integer.compare(x.getId(), y.getId());});
        for (GTable i:tables) {
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
            nrows += compare(humpback, mctable, stor, result);
            ntables++;
        }
        _log.debug("{} tables compared, {} rows compared, {} difference found", ntables, nrows, result.size());
    }

    private long compareSingle(Humpback humpback, MinkeCache cache, StorageEngine stor, List<Line> result) {
        MinkeCacheTable mctable = (MinkeCacheTable)cache.getTable(this.tableId);
        if (mctable == null) {
            throw new OrcaException("table {} is not found", this.tableId); 
        }
        return compare(humpback, mctable, stor, result);
    }

    private long compare(Humpback humpback, MinkeCacheTable mctable, StorageEngine stor, List<Line> result) {
        long count = 0;
        StorageTable stable = stor.getTable(mctable.getId());
        if (!mctable.isLoaded()) {
            return 0;
        }
        else if (stable == null) {
            _log.error("storage table {} is not found", mctable.getId());
        } 
        else {
            _log.debug("comparing {} ...", mctable.getId());
            int before = result.size();
            GTable gtable = humpback.getTable(mctable.getId());
            String cacheMode = "";
            synchronized(mctable) {
                if (mctable.isFullCache()) {
                    count = compareFullCached(mctable, gtable, stable, result);
                    cacheMode = "full";
                }
                else if (this.comparePartiallyCached) {
                    count = comparePartialCached(mctable, gtable, stable, result);
                    cacheMode = "partial";
                }
            }
            for (int i=before; i<result.size(); i++) {
                Line ii = result.get(i);
                ii.TABLE_TYPE = gtable.getTableType().toString();
                ii.CACHE_MODE = cacheMode;
            }
            _log.debug("comparison of table {} is completed count={} diff={}", 
                    mctable.getId(),
                    count,
                    result.size() - before);
        }
        return count;
    }
    
    private long compareFullCached(MinkeCacheTable mctable, GTable gtable, StorageTable stable, List<Line> result) {
        return compareRange(gtable, stable, 0, 0, 0, result);
    }

    private long comparePartialCached(MinkeCacheTable mctable, GTable gtable, StorageTable stable, List<Line> result) {
        long count = 0;
        Supplier<Range> i=mctable.getMinkeTable().scanRanges(true);
        boolean isIndex = gtable.getTableType() != TableType.DATA;
        for (;;) {
            Range ii = i.get();
            if (ii == null) break;
            if (ii.isDot()) {
                long pKey = ii.getStart().getKey();
                long pData = isIndex ? stable.getIndex(pKey, 0, null) : stable.get(pKey, 0, null);
                if (pData == 0) {
                    Line line = new Line();
                    line.TABLE_ID = gtable.getId();
                    line.IN_CACHE = 1;
                    line.IN_STORAGE = 0;
                    line.KEY = KeyBytes.toString(pKey);
                    result.add(line);
                    if (result.size() >= 1000) {
                        // too much result
                        throw new JumpException();
                    }
                }
                count++;
            }
            else {
                long options = 0;
                options = ii.getStartMark()==PLUS ? ScanOptions.excludeStart(options) : options;
                options = ii.getEndMark()==MINUS ? ScanOptions.excludeEnd(options) : options;
                count += compareRange(gtable, stable, ii.getStartKey(), ii.getEndKey(), options, result);
            }
        }
        return count;
    }

    private long compareRange(GTable gtable, 
                              StorageTable stable, 
                              long pStartKey, 
                              long pEndKey, 
                              long options,
                              List<Line> result) {
        final AtomicLong count = new AtomicLong();
        RowIterator scanx = gtable.scan(0, Long.MAX_VALUE, pStartKey, pEndKey, options);
        ScanResult scany = stable.scan(pStartKey, pEndKey, options);
        Differ<Long> differ = new Differ<>(
                new HumpbackRowSupperlier(scanx), 
                new StorageRowSupperlier(scany), 
                new MyComparator());
        differ.diff((state, x,y) -> {
            if (state == Differ.State.ADD) {
                Line line = new Line();
                line.TABLE_ID = gtable.getId();
                line.TABLE_TYPE = gtable.getTableType().toString();
                line.IN_CACHE = 0;
                line.IN_STORAGE = 1;
                line.KEY = KeyBytes.toString(y);
                result.add(line);
            }
            else if (state == Differ.State.DELETE) {
                Line line = new Line();
                line.TABLE_ID = gtable.getId();
                line.TABLE_TYPE = gtable.getTableType().toString();
                line.IN_CACHE = 1;
                line.IN_STORAGE = 0;
                line.KEY = KeyBytes.toString(x);
                result.add(line);
            }
            count.getAndIncrement();
            if (result.size() >= 1000) {
                // too much result
                throw new JumpException();
            }
        });
        scanx.close();
        scany.close();
        return count.get();
    }
}
