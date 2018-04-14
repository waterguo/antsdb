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
package com.antsdb.saltedfish.sql.planner;

import java.util.ArrayList;
import java.util.List;

import com.antsdb.saltedfish.sql.vdm.CursorIndexRangeScan;
import com.antsdb.saltedfish.sql.vdm.CursorMaker;
import com.antsdb.saltedfish.sql.vdm.CursorPrimaryKeySeek;
import com.antsdb.saltedfish.sql.vdm.DumbSorter;
import com.antsdb.saltedfish.sql.vdm.FieldMeta;
import com.antsdb.saltedfish.sql.vdm.Filter;
import com.antsdb.saltedfish.sql.vdm.FullTextIndexMergeScan;
import com.antsdb.saltedfish.sql.vdm.IndexRangeScan;
import com.antsdb.saltedfish.sql.vdm.IndexSeek;
import com.antsdb.saltedfish.sql.vdm.Operator;
import com.antsdb.saltedfish.sql.vdm.RangeScannable;
import com.antsdb.saltedfish.sql.vdm.RecordLocker;
import com.antsdb.saltedfish.sql.vdm.TableRangeScan;
import com.antsdb.saltedfish.sql.vdm.TableScan;
import com.antsdb.saltedfish.sql.vdm.TableSeek;
import com.antsdb.saltedfish.sql.vdm.UncertainScan;
import com.antsdb.saltedfish.sql.vdm.Union;
import com.antsdb.saltedfish.util.CodingError;

/**
 * Link is a logic step that walks from one Node to another
 *  
 * @author wgu0
 */
class Link {
    Node to;
    CursorMaker maker;
    Link previous;
    List<ColumnFilter> consumed = new ArrayList<>();
    Operator join;
    boolean isUnion = false;
    
    Link(Node to) {
        this.to = to;
    }
    
    boolean exists(Node node) {
        if (node == this.to) {
            return true;
        }
        else {
            if (this.previous != null) {
                return this.previous.exists(node);
            }
            else {
                return false;
            }
        }
    }

    PlannerField findField(FieldMeta field) {
        PlannerField result = this.to.findField(field);
        if (result != null) {
            return result;
        }
        if (this.previous != null) {
            result = this.previous.findField(field);
        }
        return result;
    }

    @Override
    public String toString() {
        return this.to.toString();
    }
    
    int getLevels() {
        if (this.previous != null) {
            return this.previous.getLevels() + 1;
        }
        return 1;
    }

    float getScore() {
        if (this.to.table == null) {
            // not a table. source is a cursor/subquery
            return 10;
        }
        return getScore(this.maker);
    }

    private float getScore(CursorMaker maker) {
    	    float score = 10000;
        if (maker instanceof TableSeek) {
            score = 1;
        }
        else if (maker instanceof CursorPrimaryKeySeek) {
        	    score = 1.05f;
        }
        else if (maker instanceof IndexSeek) {
        	    score = 1.1f;
        }
        else if (maker instanceof TableRangeScan) {
        	    int factor = getMatchedColumns((RangeScannable)maker);
            score = 10 - factor * 0.1f;
        }
        else if (maker instanceof IndexRangeScan) {
        	    int factor = getMatchedColumns((RangeScannable)maker);
            score = 10 - factor * 0.1f;
        }
        else if (maker instanceof CursorIndexRangeScan) {
            score = 3;
        }
        else if (maker instanceof TableScan) {
            score = 100;
        }
        else if (maker instanceof UncertainScan) {
            score = 10;
        }
        else if (maker instanceof Filter) {
        	    return getScore(((Filter)maker).getUpstream());
        }
        else if (maker instanceof Union) {
            return getScore(((Union)maker).getLeft()) + getScore(((Union)maker).getRight());
        }
        else if (maker instanceof FullTextIndexMergeScan) {
            score = 1.1f;
        }
        else if (maker instanceof DumbSorter) {
            return getScore(((DumbSorter)maker).getUpstream());
        }
        else if (maker instanceof RecordLocker) {
            return getScore(((RecordLocker) maker).getUpstream());
        }
        else {
        	    throw new CodingError();
        }
    	    return score;
    }

    private int getMatchedColumns(RangeScannable maker) {
        List<Operator> values = null;
        if (maker.getFrom() != null) {
            if (maker.getFrom().getValues() != null) {
                values = maker.getFrom().getValues();
            }
        }
        if (values == null) {
            if (maker.getTo() != null) {
                if (maker.getTo().getValues() != null) {
                    values = maker.getTo().getValues();
                }
            }
        }
        return values != null ? values.size() : 0;
    }

    public boolean isUnique(List<PlannerField> key) {
        if (key == null) {
            return false;
        }
        if (this.previous != null) {
            return this.previous.isUnique(key); 
        }
        return this.to.isUnique(key);
    }
}
