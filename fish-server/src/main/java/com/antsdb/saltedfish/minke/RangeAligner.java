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

/**
 * make sure that returned ranges is within the input.
 * 
 * @author *-xguo0<@
 */
public class RangeAligner implements RangesScanResult {

    private RangesScanResult upstream;

    RangeAligner(RangesScanResult upstream) {
        this.upstream = upstream;
    }
    
    @Override
    public boolean next() {
        boolean result = this.upstream.next();
        return result;
    }

    @Override
    public Range getInput() {
        return this.upstream.getInput();
    }

    @Override
    public Range getRange() {
        Range result = this.upstream.getRange();
        Range input = getInput();
        if (Range.compare(result.pKeyStart, result.startMark, input.pKeyStart, input.startMark) < 0) {
            result = result.clone();
            result.pKeyStart = input.pKeyStart;
            result.startMark = input.startMark;
        }
        if (Range.compare(result.pKeyEnd, result.endMark, input.pKeyEnd, input.endMark) > 0) {
            result = result.clone();
            result.pKeyEnd = input.pKeyEnd;
            result.endMark = input.endMark;
        }
        return result;
    }

    @Override
    public MinkePage getPage() {
        return this.upstream.getPage();
    }

}
