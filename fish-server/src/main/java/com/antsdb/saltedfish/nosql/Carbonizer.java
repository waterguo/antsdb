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
package com.antsdb.saltedfish.nosql;

import org.slf4j.Logger;

import com.antsdb.saltedfish.util.UberUtil;

/**
 * periodically scans tablets for the opportunity to carbonize
 *  
 * @author wgu0
 */
class Carbonizer implements Runnable {
	static Logger _log = UberUtil.getThisLogger();
	
	Humpback humpback;
	
	public Carbonizer(Humpback humpback) {
		super();
		this.humpback = humpback;
	}

	@Override
	public void run() {
		// carbonize tablets
		
		try {
			for (GTable table:this.humpback.getTables()) {
				table.carbonizeIfPossible();
			}
		}
		catch (Exception x) {
			_log.error("unexpected exception", x);
		}
		
		// release unused transactions

		TransactionCollector.collect(humpback);
	}

}
