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

import com.antsdb.saltedfish.util.CursorUtil;
import com.antsdb.saltedfish.util.UberUtil;

public class DumbGrouper extends CursorMaker {
    CursorMaker upstream;
    List<Operator> exprs;
    
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
        Map<GroupKey, List<Record>> recordsByGroupKey = new LinkedHashMap<DumbGrouper.GroupKey, List<Record>>();
        try (Cursor c = this.upstream.make(ctx, params, pMaster)) {
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
                List<Record> list = recordsByGroupKey.get(key);
                if (list == null) {
                    list = new ArrayList<Record>();
                    recordsByGroupKey.put(key, list);
                }
                Record rec = Record.toRecord(pRec);
                list.add(rec);
            }
        }
        
        // store the grouped records in list
        
        List<Record> result = new ArrayList<Record>();
        for (List<Record> i:recordsByGroupKey.values()) {
            for (Record j:i) {
                result.add(j);
            }
            result.add(Marker.GROUP_END);
        }

        // prevent empty result. there are cases like select count(*) from empty_table
        
        if (result.size() == 0 && (this.exprs.size() == 0)) {
            result.add(Marker.GROUP_END);
        }
        
        // done convert the list to the cursor
        
        Cursor c = CursorUtil.toCursor(this.getCursorMeta(), result);

        // 

        return c;
    }

    @Override
    public void explain(int level, List<ExplainRecord> records) {
        ExplainRecord rec = new ExplainRecord(level, getClass().getSimpleName());
        rec.setMakerId(makerId);
        records.add(rec);
        this.upstream.explain(level+1, records);
    }

}
