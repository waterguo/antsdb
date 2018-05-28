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

import static com.antsdb.saltedfish.minke.BoundaryMark.*;

import com.antsdb.saltedfish.cpp.KeyBytes;

/**
 * 
 * @author *-xguo0<@
 */
public class FindRangesResult implements RangesScanResult {
    private MinkeTable table;
    Range input = new Range();
    boolean asc;
    Range rangeFound;
    MinkePage pageFound;
    boolean eof;
    KeyBytes keyStart;
    KeyBytes keyEnd;

    FindRangesResult(MinkeTable table) {
        this.table = table;
    }
    
    void setRange(long pKeyStart, boolean includeStart, long pKeyEnd, boolean includeEnd, boolean ascending) {
        this.asc = ascending;
        this.keyStart = KeyBytes.alloc(pKeyStart);
        this.keyEnd = KeyBytes.alloc(pKeyEnd);
        this.input = new Range(this.keyStart.getAddress(), includeStart, this.keyEnd.getAddress(), includeEnd);
    }
    
    @Override
    public boolean next() {
        if (this.eof) {
            return false;
        }
        if (this.asc) {
            return nextRangeAsc();
        }
        else {
            return nextRangeDesc();
        }
    }
    
    @Override
    public Range getInput() {
        return this.input;
    }
    
    private boolean nextRangeAsc() {
        MinkePage page = (this.pageFound != null) ? 
                this.pageFound : 
                this.table.findCeilingPage(this.input.pKeyStart, this.input.startMark);
        while (page != null) {
            long pKey = (this.rangeFound != null) ? this.rangeFound.pKeyEnd : this.input.pKeyStart;
            int mark = (this.rangeFound != null) ?  this.rangeFound.endMark + 1 : this.input.startMark;
            Range range = page.findCeilingRange(pKey, mark);
            if (range != null) {
                if (Range.compare(this.input.pKeyEnd, this.input.endMark, range.pKeyStart, range.startMark) >= 0) {
                    this.rangeFound = range;
                    this.pageFound = page;
                    return true;
                }
            }
            page = this.table.findCeilingPage(page.getEndKeyPointer(), NONE);
        }
        this.pageFound = null;
        this.rangeFound = null;
        this.eof = true;
        return false;
    }

    private boolean nextRangeDesc() {
        throw new IllegalArgumentException();
    }

    @Override
    public Range getRange() {
        return this.rangeFound;
    }
    
    @Override
    public MinkePage getPage() {
        return this.pageFound;
    }
}
