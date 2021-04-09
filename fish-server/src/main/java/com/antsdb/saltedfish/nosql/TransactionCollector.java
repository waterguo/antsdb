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
package com.antsdb.saltedfish.nosql;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.antsdb.saltedfish.util.UberUtil;

/**
 * collects aged transactions and free resource
 *  
 * @author wgu0
 */
class TransactionCollector implements Runnable {
    static Logger _log = UberUtil.getThisLogger();

    Humpback humpback;
    long oldestTrxId;
    long secondOldestTrxId;
    
    TransactionCollector(Humpback humpback, long secondOldestTrxId) {
        this.humpback = humpback;
        this.secondOldestTrxId = secondOldestTrxId;
    }

    @Override
    public void run() {
        try {
            run_();
        }
        catch (Exception x) {
            _log.error("unexpected exception", x);
        }
    }

    private void run_() {
        this.humpback.getTrxMan().freeTo(this.secondOldestTrxId);
    }

    static void collect(Humpback humpback) {
        TrxMan trxman = humpback.getTrxMan();
        long lastClosedTrxId = humpback.getLastClosedTransactionId();
        
        // save the time consuming collecting if there is not much to collect
        if (-(lastClosedTrxId - trxman.getStartTrxId()) < 1000) {
            return;
        }
        if (trxman.size() < 5000) {
            return;
        }
        
        // proceed
        collect(humpback, lastClosedTrxId);
    }
    
    static void collect(Humpback humpback, long lastClosedTrxId) {
        // render all tables up to the point
        int count = 0;
        _log.trace("start rendering up to {}", lastClosedTrxId);
        for (GTable i:humpback.getTables()) {
            count += i.memtable.render(lastClosedTrxId);
        }
        _log.trace("{} tablets have been rendered", count);

        // free transactions a little later in case there are concurrent trxman calls
        TransactionCollector collector = new TransactionCollector(humpback, lastClosedTrxId);
        humpback.getJobManager().schedule(2, TimeUnit.SECONDS, collector);
    }
}
