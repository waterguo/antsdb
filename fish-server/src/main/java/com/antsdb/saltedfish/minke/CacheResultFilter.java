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

import org.slf4j.Logger;

import com.antsdb.saltedfish.nosql.IndexRow;
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
    private long pKey = 0;
    private long pData;

    CacheResultFilter(ScanResult upstream, MinkeCacheTable mctable, Range range) {
        this.upstream = upstream;
        this.mctable = mctable;
        this.range = range;
    }
    
    @Override
    public boolean next() {
        boolean result = false;
        if(this.upstream!=null) {
            result = this.upstream.next();
        }
        
        if (result) {
            MinkeTable table = this.mctable.mtable;
            try {
                table.copyEntry(this.upstream);
                this.pKey = this.upstream.getKeyPointer();
                this.pData = table.get(this.pKey, false);
                this.count++;
            }
            catch (OutOfMinkeSpace x) {
                // ignored if cache space runs out
                this.isBroken = true;
                this.pKey = this.upstream.getKeyPointer();
                this.pData = this.upstream.getRowPointer();
                this.count++;
            }
            this.mctable.cache.cacheMiss();
        }
        else {
            this.pKey = 0;
            this.pData = 0;
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
        return this.pKey;
    }

    @Override
    public long getIndexRowKeyPointer() {
        if (this.pData == 0) return 0;
        return new IndexRow(this.pData).getRowKeyAddress();
    }

    @Override
    public long getRowPointer() {
        return this.pData;
    }

    @Override
    public byte getMisc() {
        if (this.pData == 0) {
            return 0;
        }
        return new IndexRow(this.pData).getMisc();
    }

    @Override
    public void rewind() {
        this.upstream.rewind();
    }

    @Override
    public String toString() {
        return this.upstream.toString();
    }

    @Override
    public String getLocation() {
        return this.upstream.getLocation();
    }
}
