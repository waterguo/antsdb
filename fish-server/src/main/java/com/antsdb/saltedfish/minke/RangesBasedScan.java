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

import org.apache.commons.lang.NotImplementedException;

import com.antsdb.saltedfish.nosql.ScanResult;

/**
 * 
 * @author *-xguo0<@
 */
public class RangesBasedScan extends ScanResult {
    
    private MinkeTable table;
    private boolean asc;
    private ScanResult upstream;
    private RangesScanResult ranges;
    private boolean eof;

    RangesBasedScan(MinkeTable table, Range range, boolean asc) {
        this.table = table;
        this.asc = asc;
        this.ranges = this.table.findRanges(range, asc);
        this.ranges = new RangeAligner(this.ranges);
    }
    
    @Override
    public boolean next() {
        if (this.eof) {
            return false;
        }
        for (;;) {
            if (this.upstream == null) {
                nextRange();
                if (this.upstream == null) {
                    this.eof = true;
                    return false;
                }
            }
            boolean result = this.upstream.next();
            if (result) {
                return true;
            }
            else {
                this.upstream = null;
            }
        }
    }

    @Override
    public boolean eof() {
        return this.eof;
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
        throw new NotImplementedException();
    }

    boolean nextRange() {
        while (this.ranges.next()) {
            MinkePage page = this.ranges.getPage();
            this.upstream = page.scan(this.ranges.getRange(), this.asc);
            if (this.upstream != null) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String getLocation() {
        return this.upstream.getLocation();
    }
}
