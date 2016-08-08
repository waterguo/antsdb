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
import java.util.List;

import com.antsdb.saltedfish.util.TypeSafeUtil;

/**
 * this is a really really dumb distinct row filter. it is made to work by doing the worst ever loops. i hope there
 * will be some to develop a better one. 
 * 
 * @author xguo
 *
 */
public class DumbDistinctFilter extends CursorMaker {
    CursorMaker upstream;

    static class MyCursor extends Cursor {
        Cursor upstream;
        List<Record> past = new ArrayList<Record>();
        
        public MyCursor(Cursor upstream) {
            super(upstream.getMetadata());
            this.upstream = upstream;
        }

        @Override
        public long next() {
            for (;;) {
                // eof detection
                
                long pRecord = this.upstream.next();
                if (pRecord == 0) {
                    return 0;
                }
                
                // dup detection
                
                boolean found = false;
                Record rec = Record.toRecord(pRecord);
                for (Record i:this.past) {
                    if (equal(i, rec)) {
                        found = true;
                        break;
                    }
                }
                if (found) {
                    continue;
                }
                
                // ok 
                
                this.past.add(rec);
                return pRecord;
            }
        }

        @Override
        public void close() {
            this.upstream.close();
        }

        boolean equal(Record rec1, Record rec2) {
            for (int i=0; i<getMetadata().getColumnCount(); i++) {
                Object val1 = rec1.get(i);
                Object val2 = rec2.get(i);
                if (!TypeSafeUtil.equal(val1, val2)) {
                    return false;
                }
            }
            return true;
        }
    }
    
    public DumbDistinctFilter(CursorMaker upstream) {
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
        return new MyCursor(c);
    }

}
