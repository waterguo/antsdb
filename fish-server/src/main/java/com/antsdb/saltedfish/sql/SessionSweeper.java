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

import org.slf4j.Logger;

import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.sql.vdm.Transaction;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * periodically scan sessions to determine oldest transaction and remove expired sessions
 * 
 * @author wgu0
 */
class SessionSweeper implements Runnable {
	static Logger _log = UberUtil.getThisLogger();

	Orca orca;
	
	SessionSweeper(Orca orca) {
		this.orca = orca;
	}
	
	@Override
	public void run() {
		try {
			// find out the oldest transaction
			
			long oldest = this.orca.getTrxMan().getLastTrxId() - 1;
			for (Session session:this.orca.sessions) {
				Transaction trx = session.getTransaction_();
				if (trx == null) {
					continue;
				}
				long trxid = trx.getTrxId();
				if (trxid == 0) {
					continue;
				}
				oldest = Math.max(trxid, oldest);
			}
			
			// log the trxid so that replayer knows end of the transaction window. it is critical for freeing unused 
			// transactions
			
			long lastClosed = oldest + 1;
			Humpback humpback = this.orca.getHumpback();
			if (lastClosed < humpback.getLastClosedTransactionId()) {
				humpback.getGobbler().logTransactionWindow(lastClosed);
				humpback.setLastClosedTransactionId(lastClosed);
			}
		}
		catch (Exception x) {
			_log.error("unexpected exception", x);
		}
	}

}
