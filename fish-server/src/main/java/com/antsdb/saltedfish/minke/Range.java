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

/**
 * 
 * @author *-xguo0<@
 */
public final class Range {
    long pKeyStart;
    int startMark;
    long pKeyEnd;
    int endMark;
    
    public Range() {
    }
    
    public Range(long pStartKey, boolean incStart, long pEndKey, boolean incEnd) {
        this.pKeyStart = (pStartKey == 0) ? KeyBytes.getMinKey() : pStartKey;
        this.startMark = (incStart) ? BoundaryMark.NONE : BoundaryMark.PLUS;
        this.pKeyEnd = (pEndKey == 0) ? KeyBytes.getMaxKey() : pEndKey;
        this.endMark = (incEnd) ? BoundaryMark.NONE : BoundaryMark.MINUS;
    }
    
    public Range(long pStartKey, int startMark, long pEndKey, int endMark) {
        this.pKeyStart = (pStartKey == 0) ? KeyBytes.getMinKey() : pStartKey;
        this.startMark = startMark;
        this.pKeyEnd = (pEndKey == 0) ? KeyBytes.getMaxKey() : pEndKey;
        this.endMark = endMark;
    }
    
    public Boundary getStart() {
        Boundary result = new Boundary();
        result.pKey = this.pKeyStart;
        result.mark = this.startMark;
        return result;
    }
    
    public Boundary getEnd() {
        Boundary result = new Boundary();
        result.pKey = this.pKeyEnd;
        result.mark = this.endMark;
        return result;
    }

    public long getStartKey() {
        return this.pKeyStart;
    }
    
    public int getStartMark() {
        return this.startMark;
    }
    
    public long getEndKey() {
        return this.pKeyEnd;
    }
    
    public int getEndMark() {
        return this.endMark;
    }
    
    public boolean isGreater(Boundary boundary) {
        int cmp = compare(boundary.pKey, boundary.mark, this.pKeyEnd, this.endMark);
        return cmp > 0;
    }

    public boolean isEmpty() {
        int cmp = compare(this.pKeyEnd, this.endMark, this.pKeyStart, this.startMark);
        if (cmp != 0) {
            return cmp < 0;
        }
        if (KeyBytes.compare(this.pKeyStart, KeyBytes.getMinKey()) == 0) {
            return true;
        }
        if (KeyBytes.compare(this.pKeyStart, KeyBytes.getMaxKey()) == 0) {
            return true;
        }
        if (this.startMark == this.endMark) {
            return (this.startMark != 0);
        }
        else {
            return this.startMark > this.endMark;
        }
    }

    public boolean isDot() {
        return compare(this.pKeyStart, this.startMark, this.pKeyEnd, endMark) == 0;
    }
    
    public Range intersect(Range that) {
        Range result = new Range();
        int cmp = compare(this.pKeyStart, this.startMark, that.pKeyStart, that.startMark);
        if (cmp > 0) {
            result.pKeyStart = this.pKeyStart;
            result.startMark = this.startMark;
        }
        else if (cmp < 0) {
            result.pKeyStart = that.pKeyStart;
            result.startMark = that.startMark;
        }
        else {
            // cmp == 0
            result.pKeyStart = this.pKeyStart;
            result.startMark = (byte)Math.max(this.startMark, that.startMark);
        }
        cmp = compare(this.pKeyEnd, this.endMark, that.pKeyEnd, that.endMark);
        if (cmp > 0) {
            result.pKeyEnd = that.pKeyEnd;
            result.endMark = that.endMark;
        }
        else if (cmp < 0) {
            result.pKeyEnd = this.pKeyEnd;
            result.endMark = this.endMark;
        }
        else {
            // cmp == 0
            result.pKeyEnd = this.pKeyEnd;
            result.endMark = (byte)Math.min(this.endMark,  that.endMark);
        }
        if (!result.isValid()) {
            throw new IllegalArgumentException(result.toString());
        }
        return result;
    }

    public boolean isValid() {
        if ((this.startMark < -1) || (this.startMark > 1)) {
            return false;
        }
        if ((this.endMark < -1) || (this.endMark > 1)) {
            return false;
        }
        int cmp = compare(this.pKeyStart, this.startMark, this.pKeyEnd, this.endMark);
        return cmp <= 0;
    }

    public Range minus(Range that) {
        // suppose this=AB that=CD, CD must be inside AB
        int cmpStart = compare(this.pKeyStart, this.startMark, that.pKeyStart, that.startMark);
        int cmpEnd = compare(this.pKeyEnd, this.endMark, that.pKeyEnd, that.endMark);
        if ((cmpStart != 0) && (cmpEnd != 0)) {
            // one of the boundary must be same
            throw new IllegalArgumentException();
        }
        if (cmpStart > 0) {
            // CABD wrong
            throw new IllegalArgumentException();
        }
        if (cmpEnd < 0) {
            // ABCD wrong
            throw new IllegalArgumentException(this.toString() + "," + that.toString());
        }
        Range result = new Range();
        if (cmpStart == 0) {
            result.pKeyStart = that.pKeyEnd;
            result.startMark = that.endMark + 1;
            result.pKeyEnd = this.pKeyEnd;
            result.endMark = this.endMark;
        }
        else {
            // cmdEnd == 0
            result.pKeyStart = this.pKeyStart;
            result.startMark = this.startMark;
            result.pKeyEnd = that.pKeyStart;
            result.endMark = that.startMark-1;
        }
        return result;
    }
    
    public static int compare(long px, int xmark, long py, int ymark) {
        int cmp = compare(px, py);
        if (cmp != 0) {
            return cmp;
        }
        if (compare(px, KeyBytes.getMinKey()) == 0) {
            return 0;
        }
        if (compare(px, KeyBytes.getMaxKey()) == 0) {
            return 0;
        }
        return Integer.compare(xmark, ymark);
    }
    
    private static int compare(long px, long py) {
        return KeyBytes.compare(px, py);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        Range that = (Range)obj;
        if (compare(this.pKeyStart, this.startMark, that.pKeyStart, that.startMark) != 0) {
            return false;
        }
        if (compare(this.pKeyEnd, this.endMark, that.pKeyEnd, that.endMark) != 0) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        String start = Boundary.toString(this.pKeyStart, this.startMark);
        String end = Boundary.toString(this.pKeyEnd, this.endMark);
        String result = String.format("%s ~ %s", start, end);
        return result;
    }

    public boolean contains(Boundary boundary) {
        if (compare(boundary.pKey, boundary.mark, this.pKeyStart, this.startMark) < 0) {
            return false;
        }
        if (compare(boundary.pKey, boundary.mark, this.pKeyEnd, this.endMark) > 0) {
            return false;
        }
        return true;
    }

    public boolean contains(long pKey) {
        if (compare(pKey, BoundaryMark.NONE, this.pKeyStart, this.startMark) < 0) {
            return false;
        }
        if (compare(pKey, BoundaryMark.NONE, this.pKeyEnd, this.endMark) > 0) {
            return false;
        }
        return true;
    }
    
    /**
     * test if this range is in the provided range
     * @param range
     * @return
     */
    public boolean in(Range range) {
        return range.contains(getStart()) && range.contains(getEnd());
    }

    public void assign(Range that) {
        this.pKeyStart = that.pKeyStart;
        this.startMark = that.startMark;
        this.pKeyEnd = that.pKeyEnd;
        this.endMark = that.endMark;
    }

    public boolean hasIntersection(Range x) {
        if (x.getStart().in(this)) {
            return true;
        }
        if (x.getEnd().in(this)) {
            return true;
        }
        if (getStart().in(x)) {
            return true;
        }
        if (getEnd().in(x)) {
            return true;
        }
        return false;
    }

    public boolean larger(long pKey) {
        int result = KeyBytes.compare(pKey, this.pKeyStart);
        if (result < 0) {
            return true;
        }
        else if (result > 0) {
            return false;
        }
        else {
            // result == 0
            return this.startMark == BoundaryMark.PLUS; 
        }
    }
    
    @Override
    public Range clone() {
        Range result = new Range();
        result.pKeyStart = this.pKeyStart;
        result.startMark = this.startMark;
        result.pKeyEnd = this.pKeyEnd;
        result.endMark = this.endMark;
        return result;
    }
}
