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

import org.slf4j.Logger;

import com.antsdb.saltedfish.nosql.ScanResult;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author *-xguo0<@
 */
class CacheResultFilter extends ScanResult {
    static final Logger _log = UberUtil.getThisLogger();
    
    private ScanResult upstream;
    private MinkeCacheTable mctable;
    private Range range;
    private boolean isBroken = false;
    private int count = 0;

    CacheResultFilter(ScanResult upstream, MinkeCacheTable mctable, Range range) {
        this.upstream = upstream;
        this.mctable = mctable;
        this.range = range;
    }
    
    @Override
    public boolean next() {
        boolean result = this.upstream.next();
        if (result) {
            MinkeTable table = this.mctable.mtable;
            try {
                table.copyEntry(this);
                this.count++;
            }
            catch (OutOfMinkeSpace x) {
                // ignored if cache space runs out
                this.isBroken = true;
            }
            this.mctable.cache.cacheMiss();
        }
        else {
            if (!this.isBroken) {
                try {
                    this.mctable.mtable.putRange(range);
                    _log.debug("fetched {} from storage count={} tableId={}", 
                            range.toString(), 
                            this.count,
                            this.mctable.getId());
                }
                catch (OutOfMinkeSpace x) {
                }
            }
        }
        return result;
    }

    @Override
    public boolean eof() {
        return this.upstream.eof();
    }

    @Override
    public void close() {
        this.upstream.close();
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
    public String toString() {
        return this.upstream.toString();
    }

}
