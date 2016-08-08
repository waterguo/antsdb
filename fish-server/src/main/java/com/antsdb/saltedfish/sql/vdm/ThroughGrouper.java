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

/**
 * grouper when there is not GROUP BY clause
 * 
 * @author wgu0
 */
public class ThroughGrouper extends CursorMaker {
    CursorMaker upstream;

    static class MyCursor extends Cursor {
    	Cursor upstream;
    	boolean isEof = false;
    	
		public MyCursor(Cursor upstream) {
			super(upstream.meta);
			this.upstream = upstream;
		}

		@Override
		public long next() {
			long pRecord = this.upstream.next();
			if (pRecord != 0) {
				return pRecord;
			}
			if (!this.isEof) {
				this.isEof = true;
				return Record.GROUP_END;
			}
			return 0;
		}

		@Override
		public void close() {
			this.upstream.close();
		}

    }
    
	public ThroughGrouper(CursorMaker upstream) {
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
    	c = new MyCursor(c);
    	return c;
    }

    @Override
    public void explain(int level, List<ExplainRecord> records) {
        ExplainRecord rec = new ExplainRecord(level, getClass().getSimpleName());
        records.add(rec);
        this.upstream.explain(level+1, records);
    }

    public CursorMaker getUpstream() {
    	return this.upstream;
    }
}
