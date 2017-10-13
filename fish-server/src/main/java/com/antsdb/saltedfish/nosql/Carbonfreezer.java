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

import java.io.IOException;

import org.slf4j.Logger;

import static com.antsdb.saltedfish.util.SizeConstants.*;

import com.antsdb.saltedfish.util.UberUtil;

/**
 * periodically scans tablets for the opportunity to carbonfreeze
 *  
 * @author wgu0
 */
class Carbonfreezer implements Runnable {
	static Logger _log = UberUtil.getThisLogger();
	
	Humpback humpback;
	
	public Carbonfreezer(Humpback humpback) {
		super();
		this.humpback = humpback;
	}

	@Override
	public synchronized void run() {
		// carbonfreeze tablets
		
		try {
		    long oldestTrxId = this.humpback.getLastClosedTransactionId();
		    run(oldestTrxId, false);
		}
		catch (Exception x) {
			_log.error("unexpected exception", x);
		}
		
		// release unused transactions

		TransactionCollector.collect(humpback);
	}
	
	public synchronized int run(long oldestTrxId, boolean force) throws IOException {
	    _log.trace("starting carbonfreezing with trxid={} force={}", oldestTrxId, force);
	    
	    // carbonfreeze all ready tablets
	    
        int count = 0;
        for (GTable table:this.humpback.getTables()) {
            for (MemTablet i:table.memtable.getTablets()) {
                if (i.carbonfreeze(oldestTrxId, force)) {
                    count++;
                }
            }
        }
        // if there is no matured tablets, carbonfreeze one that holds more athan 2gb log data
        
        for (GTable table:this.humpback.getTables()) {
            for (MemTablet i:table.memtable.getTablets()) {
                if (i.isCarbonfrozen()) {
                    continue;
                }
                long holdSize = TabletUtil.getHoldDataSize(i);
                if (holdSize >= gb(2)) {
                    if (i.carbonfreeze(oldestTrxId, true)) {
                        count++;
                    }
                }
            }
        }
        
        // done
        
        if (count != 0) {
            _log.debug("{} tablets have been carbonfrozen", count);
        }
        _log.trace("carbonfreezing is finished");
        return count;
	}
}
