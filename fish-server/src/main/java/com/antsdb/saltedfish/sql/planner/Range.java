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
package com.antsdb.saltedfish.sql.planner;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;

import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.meta.IndexMeta;
import com.antsdb.saltedfish.sql.meta.PrimaryKeyMeta;
import com.antsdb.saltedfish.sql.meta.RuleMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.sql.vdm.Constants;
import com.antsdb.saltedfish.sql.vdm.CursorIndexRangeScan;
import com.antsdb.saltedfish.sql.vdm.CursorMaker;
import com.antsdb.saltedfish.sql.vdm.CursorPrimaryKeySeek;
import com.antsdb.saltedfish.sql.vdm.DumbDistinctFilter;
import com.antsdb.saltedfish.sql.vdm.IndexRangeScan;
import com.antsdb.saltedfish.sql.vdm.IndexSeek;
import com.antsdb.saltedfish.sql.vdm.Operator;
import com.antsdb.saltedfish.sql.vdm.RangeScannable;
import com.antsdb.saltedfish.sql.vdm.Seekable;
import com.antsdb.saltedfish.sql.vdm.TableRangeScan;
import com.antsdb.saltedfish.sql.vdm.TableSeek;
import com.antsdb.saltedfish.sql.vdm.UncertainScan;
import com.antsdb.saltedfish.sql.vdm.Vector;
import com.antsdb.saltedfish.util.CodingError;

/**
 * 
 * @author wgu0
 */
final class Range {
    ColumnFilter[] from;
    ColumnFilter[] to;
    boolean fromInclusive;
    boolean toInclusive;
    boolean isFromNullable = false;
    boolean isToNullable = false;
    boolean isSelectIn = false;

    Range(int size) {
        this.from = new ColumnFilter[size];
        this.to = new ColumnFilter[size];
    }

    boolean addFilter(int idx, ColumnFilter filter) {
        // if last element in the range results in a range scan, exit
        
        if (isLastElementRange(idx)) {
            // range can narrowed down further as long the current range is a
            // seekable value
            return false;
        }
        
        // process operators
        
        if (filter.op == FilterOp.EQUAL) {
            if (!addFrom(idx, filter, true)) {
                return false;
            }
            return addTo(idx, filter, true);
        }
        else if (filter.op == FilterOp.EQUALNULL) {
            if (!addFrom(idx, filter, true)) {
                return false;
            }
            return addTo(idx, filter, true);
        }
        else if (filter.op == FilterOp.LESS) {
            return addTo(idx, filter, false);
        }
        else if (filter.op == FilterOp.LESSEQUAL) {
            return addTo(idx, filter, true);
        }
        else if (filter.op == FilterOp.LARGER) {
            return addFrom(idx, filter, false);
        }
        else if (filter.op == FilterOp.LARGEREQUAL) {
            return addFrom(idx, filter, true);
        }
        else if (filter.op == FilterOp.INSELECT) {
            if (!addFrom(idx, filter, true)) {
                return false;
            }
            this.isSelectIn = true;
            return addTo(idx, filter, true);
        }
        else if (filter.op == FilterOp.INVALUES) {
            if (!addFrom(idx, filter, true)) {
                return false;
            }
            this.isSelectIn = true;
            return addTo(idx, filter, true);
        }
        else if (filter.op == FilterOp.LIKE) {
            return addFrom(idx, filter, true);
        }
        return false;
    }

    private boolean isLastElementRange(int idx) {
        if (idx == 0) {
            return false;
        }
        if (this.isSelectIn) {
            return true;
        }
        if (this.isFromNullable) {
            return true;
        }
        if (this.isToNullable) {
            return true;
        }
        return (this.from[idx - 1] != this.to[idx - 1]);
    }

    private boolean addFrom(int keyColumnPos, ColumnFilter filter, boolean inclusive) {
        ColumnFilter existing = this.from[keyColumnPos];
        if (existing != null) {
            return false;
        }
        if (filter.op == FilterOp.EQUALNULL) {
            this.isFromNullable = true;
        }
        this.fromInclusive = inclusive;
        this.from[keyColumnPos] = filter;
        return true;
    }

    private boolean addTo(int idx, ColumnFilter filter, boolean inclusive) {
        ColumnFilter existing = this.to[idx];
        if (existing == null) {
            if (filter.op == FilterOp.EQUALNULL) {
                this.isToNullable = true;
            }
            this.toInclusive = inclusive;
            this.to[idx] = filter;
            return true;
        }
        return false;
    }

    boolean isSeek() {
        if (isFromNullable || isToNullable) {
            return false;
        }
        for (int i = 0; i < this.from.length; i++) {
            if ((this.from[i] == null) || (this.to[i] == null)) {
                return false;
            }
            if (this.from[i] != this.to[i]) {
                return false;
            }
        }
        return true;
    }

    boolean isEmpty() {
        return (this.from[0] == null) && (this.to[0] == null);
    }

    public CursorMaker createMaker(TableMeta table, RuleMeta<?> key, GeneratorContext ctx) {
        if (isEmpty()) {
            return null;
        }
        boolean isSeek = isSeek();
        if (isSelectIn) {
            CursorMaker result = null;
            if (key instanceof PrimaryKeyMeta) {
                CursorPrimaryKeySeek maker = new CursorPrimaryKeySeek(table, ctx.getNextMakerId());
                setKey(maker);
                result = maker;
            }
            else {
                CursorIndexRangeScan maker = new CursorIndexRangeScan(table, (IndexMeta) key, ctx.getNextMakerId());
                setKey(maker);
                result = maker;
            }
            return result;
        }
        else if (key instanceof PrimaryKeyMeta) {
            if (isSeek) {
                TableSeek maker = new TableSeek(table, ctx.getNextMakerId());
                setKey(maker);
                return maker;
            }
            else {
                TableRangeScan maker = new TableRangeScan(table, ctx.getNextMakerId());
                setRange(maker);
                return maker;
            }
        }
        else if (key instanceof IndexMeta) {
            IndexMeta index = (IndexMeta) key;
            if (index.isUnique() && isSeek) {
                IndexSeek maker = new IndexSeek(table, index, ctx.getNextMakerId());
                setKey(maker);
                return maker;
            }
            else if (isSingleLike()) {
                Operator like = this.from[0].source;
                Operator operand = (Operator)this.from[0].operand;
                UncertainScan maker = new UncertainScan(table, index, ctx.getNextMakerId(), like, operand);
                return maker;
            }
            else {
                IndexRangeScan maker = new IndexRangeScan(table, index, ctx.getNextMakerId());
                setRange(maker);
                return maker;
            }
        }
        else {
            throw new CodingError();
        }
    }

    private boolean isSingleLike() {
        if (this.from.length > 1 && this.from[1] != null) {
            return false;
        }
        if (this.from[0] == null) {
            return false;
        }
        return this.from[0].op == FilterOp.LIKE;
    }

    private void setKey(Seekable maker) {
        maker.setSeekKey(new Vector(getValues(from), this.fromInclusive, this.isFromNullable));
    }

    private void setRange(RangeScannable maker) {
        maker.setFrom(new Vector(getValues(from), this.fromInclusive, this.isFromNullable));
        maker.setTo(new Vector(getValues(to), this.toInclusive, this.isToNullable));
    }

    private void setKey(CursorPrimaryKeySeek maker) {
        Vector key = new Vector(getValues(this.from), this.fromInclusive, this.isFromNullable);
        CursorMaker select = findSelect(this.from);
        select = new DumbDistinctFilter(select);
        maker.setKey(key, select);
    }

    private void setKey(CursorIndexRangeScan maker) {
        Vector key = new Vector(getValues(this.from), this.fromInclusive, this.isFromNullable);
        CursorMaker select = findSelect(this.from);
        select = new DumbDistinctFilter(select);
        maker.setKey(key, select);
    }

    @SuppressWarnings("unchecked")
    private CursorMaker findSelect(ColumnFilter[] values) {
        CursorMaker select = null;
        for (ColumnFilter i : values) {
            if (i.op == FilterOp.INSELECT) {
                select = (CursorMaker) i.operand;
                break;
            }
            if (i.op == FilterOp.INVALUES) {
                Constants constants = new Constants((List<Operator>) i.operand, "");
                select = constants;
                break;
            }
        }
        return select;
    }
    
    private List<Operator> getValues(ColumnFilter[] values) {
        List<Operator> result = new ArrayList<>();
        for (ColumnFilter i : values) {
            if (i == null) {
                break;
            }
            if ((i.op == FilterOp.INSELECT) || (i.op == FilterOp.INVALUES)) {
                break;
            }
            result.add((Operator)i.operand);
        }
        return result;
    }

    public Path createPath(QueryNode node, RuleMeta<?> key, List<ColumnFilter> consumed) {
        if (key instanceof PrimaryKeyMeta) {
            return new PathSeek(key, node, consumed);
        }
        else if (key instanceof IndexMeta) {
            return new PathRangeScan(key, node, consumed);
        }
        throw new NotImplementedException();
    }

}
