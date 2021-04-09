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
package com.antsdb.saltedfish.sql.vdm;

import java.util.ArrayList;
import java.util.List;

import com.antsdb.saltedfish.sql.planner.SortKey;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * group records that flows in according to the group key
 *  
 * @author *-xguo0<@
 */
public class SequentialGrouper extends CursorMaker {
    CursorMaker upstream;
    private List<Operator> exprs;

    static class GroupKey {
        List<Object> values = new ArrayList<Object>();

        @Override
        public int hashCode() {
            int hash = 0;
            for (int i=0; i<this.values.size(); i++) {
                Object value = this.values.get(i);
                if (value == null) {
                    continue;
                }
                hash = hash ^ value.hashCode();
            }
            return hash;
        }
        
        @Override
        public boolean equals(Object obj) {
            GroupKey another = (GroupKey)obj;
            if (another == null) {
                return false;
            }
            if (another.values.size() != this.values.size()) {
                return false;
            }
            
            for (int i=0; i<this.values.size(); i++) {
                if (!UberUtil.safeEqual(this.values.get(i), another.values.get(i))) {
                    return false;
                }
            }
            return true;
        }
     }
    
    private class MyCursor extends CursorWithHeap {
        private Cursor upstream;
        private VdmContext ctx;
        private Parameters params;
        private GroupKey nextKey;
        private long pNextRecord;
        private boolean eof = false;
        
        public MyCursor(VdmContext ctx, Parameters params, Cursor upstream) {
            super(upstream.getMetadata());
            this.upstream = upstream;
            this.ctx = ctx;
            this.params = params;
        }

        @Override
        public long next() {
            if (this.pNextRecord != 0) {
                long pResult = this.pNextRecord;
                this.pNextRecord = 0;
                return pResult;
            }
            if (this.eof) {
                return 0;
            }
            long pCurrentRecord = this.upstream.next();
            if (pCurrentRecord == 0) {
                this.eof = true;
                return 0;
            }
            GroupKey key = getGroupKey(exprs, this.ctx, this.params, pCurrentRecord);
            if (this.nextKey == null) {
                this.nextKey = key;
                return pCurrentRecord;
            }
            else if (!key.equals(this.nextKey)) {
                this.nextKey = key;
                this.pNextRecord = pCurrentRecord;
                return 0;
            }
            else {
                return pCurrentRecord;
            }
        }

        @Override
        public void close() {
            super.close();
            this.upstream.close();
        }
    }
    
    public SequentialGrouper(CursorMaker upstream, List<Operator> exprs, int makerId) {
        this.upstream = upstream;
        this.exprs = exprs;
        setMakerId(makerId);
    }
    
    @Override
    public CursorMeta getCursorMeta() {
        return this.upstream.getCursorMeta();
    }

    @Override
    public boolean setSortingOrder(List<SortKey> order) {
        return false;
    }

    @Override
    public float getScore() {
        return this.upstream.getScore();
    }
    
    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        Cursor upstream = this.upstream.make(ctx, params, pMaster);
        return new MyCursor(ctx, params, upstream);
    }

    @Override
    public void explain(int level, List<ExplainRecord> records) {
        super.explain(level, records);
        this.upstream.explain(level+1, records);
    }
    
    static GroupKey getGroupKey(List<Operator> exprs, VdmContext ctx, Parameters params, long pRecord) {
        GroupKey key = new GroupKey();
        if (exprs != null) { 
            for (Operator i:exprs) {
                Object val = Util.eval(ctx, i, params, pRecord);
                key.values.add(val);
            }
        }
        return key;
    }}
