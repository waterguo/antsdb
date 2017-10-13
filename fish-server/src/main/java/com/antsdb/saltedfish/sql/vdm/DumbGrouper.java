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
package com.antsdb.saltedfish.sql.vdm;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.antsdb.saltedfish.util.UberUtil;

public class DumbGrouper extends CursorMaker {
    static final long GROUP_END = Record.GROUP_END;
    CursorMaker upstream;
    List<Operator> exprs;
    
    private static class MyCursor extends Cursor {
        private List<Long> items;
        private Cursor upstream;
        int i=0;

        public MyCursor(Cursor upstream, List<Long> items) {
            super(upstream.getMetadata());
            this.items = items;
            this.upstream = upstream;
        }

        @Override
        public long next() {
            if (i >= this.items.size()) {
                return 0;
            }
            long pResult = items.get(this.i);
            this.i++;
            return pResult;
        }

        @Override
        public void close() {
            this.upstream.close();
        }
        
    }
    
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
    
    public DumbGrouper(CursorMaker upstream, List<Operator> exprs, int makerId) {
        super();
        this.upstream = upstream;
        this.exprs = exprs;
        setMakerId(makerId);
    }

    @Override
    public CursorMeta getCursorMeta() {
        return this.upstream.getCursorMeta();
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        
        // fetch all rows in memory and group them by group key
        
    	AtomicLong counter = ctx.getCursorStats(makerId);
        Map<GroupKey, List<Long>> recordsByGroupKey = new LinkedHashMap<DumbGrouper.GroupKey, List<Long>>();
        Cursor c = this.upstream.make(ctx, params, pMaster);
        c = new RecordBuffer(c);
        for (;;) {
            long pRec = c.next();
            if (pRec == 0) {
                break;
            }
            counter.incrementAndGet();
            GroupKey key = new GroupKey();
            if (this.exprs != null) { 
                for (Operator i:this.exprs) {
                    Object val = Util.eval(ctx, i, params, pRec);
                    key.values.add(val);
                }
            }
            List<Long> list = recordsByGroupKey.get(key);
            if (list == null) {
                list = new ArrayList<Long>();
                recordsByGroupKey.put(key, list);
            }
            list.add(pRec);
        }
        
        // store the grouped records in list
        
        List<Long> records = new ArrayList<Long>();
        for (List<Long> i:recordsByGroupKey.values()) {
            records.addAll(i);
            records.add(GROUP_END);
        }

        // prevent empty result. there are cases like select count(*) from empty_table
        
        if (records.size() == 0 && (this.exprs.size() == 0)) {
            records.add(GROUP_END);
        }
        
        // done convert the list to the cursor
        
        Cursor result = new MyCursor(c, records);
        return result;
    }

    @Override
    public void explain(int level, List<ExplainRecord> records) {
        ExplainRecord rec = new ExplainRecord(level, getClass().getSimpleName());
        rec.setMakerId(makerId);
        records.add(rec);
        this.upstream.explain(level+1, records);
    }

}
