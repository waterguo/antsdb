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

import java.util.List;

import com.antsdb.saltedfish.sql.planner.SortKey;

/**
 * purpose of this class is to collapse the grouped records into one.
 * 
 * @author xguo
 *
 */
public class GroupByPostProcesser extends CursorMaker {
    CursorMaker upstream;
    
    class GroupedCursor extends Cursor {
        Cursor upstream;
        
        public GroupedCursor(Cursor upstream) {
            super(GroupByPostProcesser.this.upstream.getCursorMeta());
            this.upstream = upstream;
        }

        @Override
        public long next() {
            long pLastRecord = 0;
            for (;;) {
                long pRecord = this.upstream.next();
                if (pRecord == 0) {
                    return 0;
                }
                if (Record.isGroupEnd(pRecord)) {
                    return pLastRecord;
                }
                pLastRecord = pRecord;
            }
        }

        @Override
        public void close() {
            this.upstream.close();
        }
    }
    
    public GroupByPostProcesser(CursorMaker upstream) {
        super();
        this.upstream = upstream;
    }

    @Override
    public CursorMeta getCursorMeta() {
        return this.upstream.getCursorMeta();
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        Cursor c = this.upstream.make(ctx, params, pMaster);
        c = new GroupedCursor(c);
        return c;
    }

    @Override
    public void explain(int level, List<ExplainRecord> records) {
        ExplainRecord rec = new ExplainRecord(getMakerid(), level, getClass().getSimpleName());
        records.add(rec);
        this.upstream.explain(level+1, records);
    }

    @Override
    public boolean setSortingOrder(List<SortKey> order) {
        return false;
    }

}
