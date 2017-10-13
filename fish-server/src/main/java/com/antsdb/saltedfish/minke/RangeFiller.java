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

/**
 * ranges scan result from MinkeTable is discreet. only the existing ranges are returned. RangeFiller will examine
 * the range according to input and fill the missing ones. keep in mind the filled range has null return value from
 * getPage() 
 * 
 * @author *-xguo0<@
 */
public class RangeFiller implements RangesScanResult {
    
    private RangesScanResult upstream;
    private long pKey;
    private int mark;
    private Range range;
    private MinkePage page;
    private boolean eof;

    RangeFiller(RangesScanResult upstream) {
        this.upstream = upstream;
        this.pKey = upstream.getInput().pKeyStart;
        this.mark = upstream.getInput().startMark;
    }
    
    @Override
    public boolean next() {
        if (eof) {
            return false;
        }
        boolean result = next_();
        if (result) {
            this.pKey = this.range.pKeyEnd;
            this.mark = this.range.endMark;
        }
        else {
            this.pKey = 0;
            this.mark = 0;
        }
        return result;
    }

    private boolean next_() {
        // if current page is a filling range
        
        if ((this.range != null) && (this.page == null)) {
            this.range = this.upstream.getRange();
            this.page = this.upstream.getPage();
            return true;
        }
        
        // if not, get next from upstream
        
        Range input = upstream.getInput();
        boolean result = this.upstream.next();
        if (result) {
            Range rangeUpstream = this.upstream.getRange();
            if (Range.compare(rangeUpstream.pKeyStart, rangeUpstream.startMark, this.pKey, this.mark) > 0) {
                this.range = new Range();
                this.range.pKeyStart = this.pKey;
                this.range.startMark = this.mark;
                this.range.pKeyEnd = rangeUpstream.pKeyStart;
                this.range.endMark = rangeUpstream.startMark - 1;
                this.page = null;
            }
            else {
                this.range = rangeUpstream;
                this.page = this.upstream.getPage();
            }
            return true;
        }
        else if (Range.compare(this.range.pKeyEnd, this.range.endMark, input.pKeyEnd, input.endMark) < 0) {
            this.range = new Range();
            this.range.pKeyStart = this.range.pKeyEnd;
            this.range.startMark = this.range.endMark + 1;
            this.range.pKeyEnd = input.pKeyEnd;
            this.range.endMark = input.endMark;
            this.page = null;
            return true;
        }
        else {
            this.eof = true;
            return false;
        }
    }
    
    @Override
    public Range getInput() {
        return this.upstream.getInput();
    }

    @Override
    public Range getRange() {
        return this.range;
    }

    @Override
    public MinkePage getPage() {
        return this.page;
    }

}
