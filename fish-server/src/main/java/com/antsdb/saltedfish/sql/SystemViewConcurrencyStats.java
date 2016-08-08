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
package com.antsdb.saltedfish.sql;

import java.util.HashMap;
import java.util.Map;

import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.MemTablet;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.sql.vdm.Cursor;
import com.antsdb.saltedfish.sql.vdm.CursorMaker;
import com.antsdb.saltedfish.sql.vdm.CursorMeta;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.Transaction;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.util.CursorUtil;

/**
 * 
 * @author wgu0
 */
public class SystemViewConcurrencyStats extends CursorMaker {
	Orca orca;
	CursorMeta meta;
	
    public static class Item {
        public int TABLE_ID;
        public String NAMESPACE;
        public String TABLE_NAME;
        public long CAS_RETRIES;
        public long LOCK_WAITS;
		public long CONCONRRENT_UPDATES;
		
		public boolean isAllZero() {
			if (this.CAS_RETRIES != 0) return false;
			if (this.LOCK_WAITS != 0) return false;
			if (this.CONCONRRENT_UPDATES != 0) return false;
			return true;
		}
    }

	public SystemViewConcurrencyStats(Orca orca) {
		this.orca = orca;
		meta = CursorUtil.toMeta(Item.class);
	}

	@Override
	public CursorMeta getCursorMeta() {
		return meta;
	}

	@Override
	public Object run(VdmContext ctx, Parameters params, long pMaster) {
		Map<Integer, Item> itemByTableId = new HashMap<>();
        for (GTable table:this.orca.getHumpback().getTables()) {
        	for (MemTablet tablet:table.getTablets()) {
        		add(itemByTableId, table, tablet);
        	}
        }

		// done
        
        Cursor c = CursorUtil.toCursor(meta, itemByTableId.values());
        return c;
	}

	private void add(Map<Integer, Item> itemByTableId, GTable gtable, MemTablet tablet) {
		Item item = itemByTableId.get(gtable.getId());
		boolean isNew = false;
		if (item == null) {
			item = new Item();
			isNew = true;
		}
		item.CAS_RETRIES += tablet.getCasRetries();
		item.LOCK_WAITS += tablet.getLockWaits();
		item.CONCONRRENT_UPDATES += tablet.getCurrentUpdates();
		if (item.isAllZero()) {
			return;
		}
		TableMeta table = this.orca.getMetaService().getTable(Transaction.getSeeEverythingTrx(), gtable.getId());
		item.TABLE_ID = gtable.getId();
		item.NAMESPACE = table.getNamespace();
		item.TABLE_NAME = table.getTableName();
		if (isNew) {
			itemByTableId.put(gtable.getId(), item);
		}
	}
}
