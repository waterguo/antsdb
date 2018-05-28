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

import java.util.List;

import com.antsdb.saltedfish.sql.planner.SortKey;

/**
 * limits the result set. mysql limit clause
 * 
 * @author wgu0
 */
public class Limiter extends CursorMaker {
	CursorMaker upstream;
	int offset;
	int limit;
	
	class LimiterCursor extends Cursor {
		Cursor upstream;
		int counter = 0;
		
		public LimiterCursor(Cursor c) {
			super(c.getMetadata());
			this.upstream = c;
			for (int i=0; i<offset; i++) {
				long pRecord = c.next();
				if (pRecord == 0) {
					this.upstream.close();
					break;
				}
			}
		}

		@Override
		public long next() {
			if (this.counter >= limit) {
				close();
				return 0;
			}
			long pRecord = this.upstream.next();
			if (pRecord == 0) {
				close();
				return 0;
			}
			this.counter++;
			return pRecord;
		}

		@Override
		public void close() {
			this.upstream.close();
		}

	}
	
    public Limiter(CursorMaker upstream, int offset, int count) {
        this.upstream = upstream;
        this.offset = offset;
        this.limit = count;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        Cursor c = this.upstream.make(ctx, params, pMaster);
        c = new LimiterCursor(c);
        return c;
    }

    @Override
    public CursorMeta getCursorMeta() {
        return this.upstream.getCursorMeta();
    }

    @Override
    public void explain(int level, List<ExplainRecord> records) {
        this.upstream.explain(level, records);
    }

    @Override
    public boolean setSortingOrder(List<SortKey> order) {
        return this.upstream.setSortingOrder(order);
    }
}
