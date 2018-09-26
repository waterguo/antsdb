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
package com.antsdb.saltedfish.sql;

import java.util.ArrayList;

import com.antsdb.saltedfish.sql.vdm.Cursor;
import com.antsdb.saltedfish.sql.vdm.Measure;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.Script;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.sql.vdm.View;
import com.antsdb.saltedfish.util.CursorUtil;

public class SystemMetrics extends View {
	Orca orca;
	
    public static class Item {
        public String SQL = "";
        public int COUNT = 0;
        public int AVERAGE_LATENCY;
        public int MIN_LATENCY;
        public int MAX_LATENCY;
    }

	public SystemMetrics(Orca orca) {
	    super(CursorUtil.toMeta(Item.class));
		this.orca = orca;
	}

	@Override
	public Object run(VdmContext ctx, Parameters params, long pMaster) {
        ArrayList<Item> list = new ArrayList<>();
        long now = System.nanoTime();
        for (Script i:this.orca.statementCache.asMap().values()) {
            Measure m = i.getMeasure();
            if (m == null) {
                continue;
            }
            Item item = new Item();
            list.add(item);
            item.SQL = i.getSql();
            if (m.isEnabled()) {
                item.COUNT = m.getCount(now);
                if (item.COUNT != 0) {
                    item.MIN_LATENCY = m.getMinLatency(now);
                    item.MAX_LATENCY = m.getMaxlatency(now);
                    item.AVERAGE_LATENCY = m.getTotalLatency(now) / item.COUNT;
                }
            }
            else {
                item.COUNT = -1;
                item.AVERAGE_LATENCY = 0;
                item.MIN_LATENCY = 0;
                item.MAX_LATENCY = 0;
            }
        }
        Cursor c = CursorUtil.toCursor(meta, list);
        return c;
	}

}
