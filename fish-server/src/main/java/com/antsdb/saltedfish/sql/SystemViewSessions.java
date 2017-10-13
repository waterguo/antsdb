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

import java.util.ArrayList;
import java.util.List;

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
public class SystemViewSessions extends CursorMaker {
	Orca orca;
	CursorMeta meta;
	
    public static class Item {
        public int ID;
		public long TRX;
		public String USER;
		public String REMOTE;
    }

	public SystemViewSessions(Orca orca) {
		this.orca = orca;
		meta = CursorUtil.toMeta(Item.class);
	}

	@Override
	public CursorMeta getCursorMeta() {
		return meta;
	}

	@Override
	public Object run(VdmContext ctx, Parameters params, long pMaster) {
        ArrayList<Item> list = new ArrayList<>();
        for (Session session:this.orca.sessions) {
            addItem(list, session);
        }
		
		// done
        Cursor c = CursorUtil.toCursor(meta, list);
        return c;
	}

	void addItem(List<Item> list, Session session) {
        Item item = new Item();
        list.add(item);
        item.ID = session.getId();
        Transaction trx = session.getTransaction_();
		item.TRX = (trx != null) ? trx.getTrxId() : 0;
		item.USER = session.getUser();
		item.REMOTE = session.remote;
	}
}
