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

import com.antsdb.saltedfish.minke.MinkePage.RangeScanner;
import com.antsdb.saltedfish.nosql.TableType;
import com.antsdb.saltedfish.nosql.VaporizingRow;

/**
 * purpose of this class is to feed the MinkeTable.mergeSplit() with rows and ranges in ascending order
 * 
 * @author *-xguo0<@
 */
class MergeStream {
    static final int WINNER_EOF = 0;
    static final int WINNER_MERGE = 1;
    static final int WINNER_NON_RANGE = 2;
    static final int WINNER_RANGE = 3;
    static final int RESULT_EOF = 0;
    static final int RESULT_DATA = 1;
    static final int RESULT_RANGE = 2;
    static final int RESULT_ROW = 3;
    static final int RESULT_INDEX = 4;
    static final int RESULT_VROW = 5;
    
    RangeScanner ranges;
    MinkePage.Scanner scanner;
    Range range;
    boolean skipNext = false;
    private long merge_pKey;
    private int merge_keyMark;
    private long merge_pData;
    private byte merge_misc;
    private int merge_dataType = RESULT_EOF;
    private Object merge_object;
    private long merge_rowKey;
    private int result_dataType;
    private long result_pKey;
    private long result_pData;
    private Object result_object;
    private int winner;
    private long result_rowKey;
    private byte result_misc;
    private TableType tableType;
    
    MergeStream(TableType tableType, MinkePage page) {
        this.ranges = page.getAllRanges();
        this.scanner = page.scanAll();
        this.range = this.ranges.next();
        if (this.scanner != null) {
            this.scanner.next();
        }
        this.tableType = tableType;
    }

    void setMergeData(VaporizingRow row) {
        this.merge_pKey = row.getKeyAddress();
        this.merge_keyMark = BoundaryMark.NONE;
        this.merge_object = row;
        this.merge_dataType = RESULT_VROW;
    }
    
    void setMergeData(long pKey, long pData) {
        this.merge_pKey = pKey;
        this.merge_keyMark = BoundaryMark.NONE;
        this.merge_pData = pData;
        this.merge_dataType = RESULT_ROW;
    }

    void setMergeIndexData(long pKey, long pRowKey, byte misc) {
        this.merge_pKey = pKey;
        this.merge_keyMark = BoundaryMark.NONE;
        this.merge_rowKey = pRowKey;
        this.merge_misc = misc;
        this.merge_dataType = RESULT_INDEX;
    }
    
    void setMergeRange(Range range) {
        this.merge_pKey = range.pKeyStart;
        this.merge_keyMark = range.startMark;
        this.merge_object = range;
        this.merge_dataType = RESULT_RANGE;
    }
    
    boolean isResultRange() {
        return this.result_dataType == RESULT_RANGE;
    }
    
    int getWinner() {
        return this.winner;
    }
    
    // this is a 3 way comparison
    boolean next() {
        long pKey = 0;
        int mark = BoundaryMark.NONE;
        this.winner = WINNER_EOF;
        
        // merge key
        
        if (merge_pKey != 0) {
            pKey = this.merge_pKey;
            mark = this.merge_keyMark;
            this.winner = WINNER_MERGE;
        }
        
        // compare with row key
        
        long pRowKey = this.scanner!=null ? this.scanner.getKeyPointer() : 0;
        if (pRowKey != 0) {
            if (pKey == 0) {
                pKey = pRowKey;
                this.winner = WINNER_NON_RANGE;
            }
            else {
                int cmp = Range.compare(pRowKey, BoundaryMark.NONE, pKey, mark);
                if (cmp < 0) {
                    pKey = pRowKey;
                    this.winner = WINNER_NON_RANGE;
                }
                else if (cmp==0 && !(this.merge_object instanceof Range)) {
                    // skip the row if it collides with merge key
                    this.scanner.next();
                }
            }
        }
        
        // compare with range key
        
        if (this.range != null) {
            if (pKey == 0) {
                pKey = this.range.pKeyStart;
                this.winner = WINNER_RANGE;
            }
            else {
                int cmp = Range.compare(this.range.pKeyStart, this.range.startMark, pKey, 0);
                if (cmp < 0) {
                    pKey = this.range.pKeyStart;
                    this.winner = WINNER_RANGE;
                }
            }
        }
        
        // winner steps forward
        
        this.result_pKey = pKey;
        if (winner == WINNER_MERGE) {
            this.result_dataType = this.merge_dataType;
            this.result_pData = this.merge_pData;
            this.result_rowKey = this.merge_rowKey;
            this.result_misc = this.merge_misc;
            this.result_object = this.merge_object;
            this.merge_pKey = 0;
            this.merge_pData = 0;
        }
        else if (winner == WINNER_NON_RANGE) {
            this.result_dataType = RESULT_DATA;
            this.result_pData = scanner.getData();
            this.scanner.next();
        }
        else if (winner == WINNER_RANGE) {
            this.result_dataType = RESULT_RANGE;
            this.result_object = this.range;
            this.range = this.ranges.next();
        }
        else {
            return false;
        }
        return true;
    }
    
    long apply(MinkePage page) {
        if (this.result_dataType == RESULT_DATA) {
            return page.put(this.tableType, this.result_pKey, this.result_pData, 0);
        }
        else if (this.result_dataType == RESULT_ROW) {
            return page.put(TableType.DATA, this.result_pKey, this.result_pData, 0);
        }
        else if (this.result_dataType == RESULT_INDEX) {
            return page.putIndex(this.result_pKey, this.result_rowKey, this.result_misc);
        }
        else if (this.result_dataType == RESULT_RANGE) {
            return page.putRange((Range)this.result_object);
        }
        else if (this.result_dataType == RESULT_VROW) {
            return page.put((VaporizingRow)this.result_object);
        }
        else {
            throw new IllegalArgumentException();
        }
    }
    
    Range getRange() {
        return (Range)this.result_object;
    }
    
    long getData() {
        return this.result_pData;
    }

    public long getKey() {
        return this.result_pKey;
    }
}
