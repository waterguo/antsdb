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

import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.nosql.ScanOptions;
import com.antsdb.saltedfish.nosql.ScanResult;
import static com.antsdb.saltedfish.minke.BoundaryMark.*;

/**
 * 
 * @author *-xguo0<@
 */
class MinkeCacheTableScanner extends ScanResult implements FindNextRangeAndDoShitCallback {
    ScanResult upstream;
    boolean isEof = false;
    MinkeCacheTable mctable;
    KeyBytes keyStart;
    KeyBytes keyEnd;
    Range left;
    Range input;
    boolean ascending;
    boolean hasCacheMiss = false;
    boolean cacheResult = true;
    
    MinkeCacheTableScanner(MinkeCacheTable mctable) {
        this.mctable = mctable;
    }
    
    void setCacheResult(boolean value) {
        this.cacheResult = value;
    }
    
    void setRange(long pKeyStart, boolean includeStart, long pKeyEnd, boolean includeEnd, boolean ascending) {
        this.ascending = ascending;
        if (pKeyStart == 0) {
            pKeyStart = ascending ? KeyBytes.getMinKey() : KeyBytes.getMaxKey();
        }
        if (pKeyEnd == 0) {
            pKeyEnd = ascending ? KeyBytes.getMaxKey() : KeyBytes.getMinKey();
        }
        this.keyStart = KeyBytes.alloc(pKeyStart);
        this.keyEnd = KeyBytes.alloc(pKeyEnd);
        this.left = new Range();
        if (ascending) {
            left.pKeyStart = this.keyStart.getAddress();
            left.pKeyEnd = this.keyEnd.getAddress();
            left.startMark = includeStart ? NONE : PLUS;
            left.endMark = includeEnd ? NONE : MINUS;
        }
        else {
            left.pKeyStart = this.keyEnd.getAddress();
            left.pKeyEnd = this.keyStart.getAddress();
            left.startMark = includeEnd ? NONE : PLUS;
            left.endMark = includeStart ? NONE : MINUS;
        }
        this.input = this.left.clone();
    }
    
    @Override
    public boolean next() {
        if (this.isEof) {
            return false;
        }
        for (;;) {
            if (this.upstream != null) {
                if (this.upstream.next()) {
                    return true;
                }
                else {
                    this.upstream.close();
                    this.upstream = null;
                }
            }
            if (nextRange()) {
                continue;
            }
            this.isEof = true;
            verifyCacheScan();
            return false;
        }
    }

    private boolean nextRange() {
        if (this.left.isEmpty()) {
            return false;
        }
        ScanResult result = this.mctable.mtable.findNextRangeAndDoShit(this.left, this, this.ascending);
        if (result == null) {
            if (!this.left.isEmpty()) {
                if (this.ascending) {
                    long options = 0;
                    options = this.left.startMark == NONE ? options : ScanOptions.excludeStart(options);
                    options = this.left.endMark == NONE ? options : ScanOptions.excludeEnd(options);
                    this.upstream = this.mctable.stable.scan(this.left.pKeyStart, this.left.pKeyEnd, options);
                    if (cacheResult()) {
                        this.upstream = new CacheResultFilter(this.upstream, mctable, this.left);
                        this.hasCacheMiss = true;
                    }
                }
                else {
                    long options = 0;
                    options = this.left.endMark == NONE ? options : ScanOptions.excludeStart(options);
                    options = this.left.startMark == NONE ? options : ScanOptions.excludeEnd(options);
                    options = ScanOptions.descending(options);
                    this.upstream = this.mctable.stable.scan(this.left.pKeyEnd, this.left.pKeyStart, options);
                    if (cacheResult()) {
                        this.upstream = new CacheResultFilter(this.upstream, mctable, this.left);
                        this.hasCacheMiss = true;
                    }
                }
                Range empty = new Range();
                empty.pKeyStart = this.left.pKeyEnd;
                empty.startMark = this.left.endMark + 1;
                empty.pKeyEnd = this.left.pKeyEnd;
                empty.endMark = this.left.endMark;
                this.left = empty;
            }
        }
        return this.upstream != null;
    }

    @Override
    public boolean eof() {
        return this.isEof;
    }

    @Override
    public void close() {
        if (this.upstream != null) {
            this.upstream.close();
            this.upstream = null;
        }
        this.isEof = true;
    }

    @Override
    public long getVersion() {
        return this.upstream.getVersion();
    }

    @Override
    public long getKeyPointer() {
        return this.upstream.getKeyPointer();
    }

    @Override
    public long getIndexRowKeyPointer() {
        return this.upstream.getIndexRowKeyPointer();
    }

    @Override
    public long getRowPointer() {
        return this.upstream.getRowPointer();
    }

    @Override
    public byte getMisc() {
        return this.upstream.getMisc();
    }

    @Override
    public void rewind() {
        this.upstream.rewind();
    }

    @Override
    public ScanResult doShit(MinkePage page, Range range, boolean isNegativeRange, boolean ascending) {
        ScanResult result;
        if (isNegativeRange) {
            if (ascending) {
                long options = 0;
                options = range.startMark == NONE ? options : ScanOptions.excludeStart(options);
                options = range.endMark == NONE ? options : ScanOptions.excludeEnd(options);
                result = this.mctable.stable.scan(range.pKeyStart, range.pKeyEnd, options);
            }
            else {
                long options = 0;
                options = range.endMark == NONE ? options : ScanOptions.excludeStart(options);
                options = range.startMark == NONE ? options : ScanOptions.excludeEnd(options);
                options = ScanOptions.descending(options);
                result = this.mctable.stable.scan(range.pKeyEnd, range.pKeyStart, options);
            }
            if ((result != null) && cacheResult()) {
                this.hasCacheMiss = true;
                result = new CacheResultFilter(result, mctable, range);
            }
        }
        else {
            result = page.scan(range, ascending);
            if (result == null) {
                result = ScanResult.emptyResult();
            }
        }
        if (result != null) {
            this.left = this.left.minus(range);
            this.upstream = result;
        }
        return result;
    }

    @Override
    public String toString() {
        return this.upstream.toString();
    }
    
    private boolean cacheResult() {
        return this.cacheResult && this.mctable.cache.isMutable;
    }

    private void verifyCacheScan() {
        if (this.mctable.cache.getVerificationMode() < (this.hasCacheMiss ? 1 : 2)) {
            return;
        }
        ScanVerifier.check(this.mctable, this.input, this.ascending);
    }

    @Override
    public String getLocation() {
        return this.upstream.getLocation();
    }

}
