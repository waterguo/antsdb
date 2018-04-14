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
        long safePoint = renderOldest(humpback);
        if (safePoint >= 0) {
            return;
        }
        _log.trace("schedule trx release up to {}", safePoint);
        TransactionCollector collector = new TransactionCollector(humpback, safePoint);
        humpback.getJobManager().schedule(2, TimeUnit.SECONDS, collector);
    }
    
    static long renderOldest(Humpback humpback) {
        // find out system wide last closed trxid
        
        long activeSessionStartTrxId = humpback.getLastClosedTransactionId() - 1;
        
        // find out the oldest tablet and the second oldest. we only collect one tablet one at a time
        
        MemTable oldest = null;
        long oldestTrxId = Long.MIN_VALUE;
        long secondOldestTrxId = Long.MIN_VALUE;
        for (GTable i:humpback.getTables()) {
            long startTrxId = i.getMemTable().getStartTrxId();
            if (startTrxId == 0) {
                continue;
            }
            if (startTrxId >= oldestTrxId) {
                secondOldestTrxId = oldestTrxId; 
                oldestTrxId = startTrxId;
                oldest = i.getMemTable();
            }
            else if (startTrxId > secondOldestTrxId) {
                secondOldestTrxId = startTrxId; 
            }
        }
        
        // render the tablet
        
        if (oldest == null) {
            return 0;
        }
        if (oldestTrxId <= activeSessionStartTrxId) {
            return 0;
        }
        // _log.debug("start rendering {} ", oldest.getId());
        oldest.render(humpback.getLastClosedTransactionId());
        long currentStartTrxId = oldest.getStartTrxId();
        _log.trace(
            "activeSessionStartTrxId={} currentStartTrxId={} oldestTrxId={} secondOldestTrxId={}",
            activeSessionStartTrxId,
            currentStartTrxId,
            oldestTrxId,
            secondOldestTrxId);
        long startTrxId = max(activeSessionStartTrxId, currentStartTrxId, secondOldestTrxId);

        // now the transactions between the oldest and endpoint are safe to free. keep in mind there
        // could be mutltiple tablets have the same start point
        
        long endpoint = startTrxId + 1;
        if (endpoint > oldestTrxId) {
            // multiple tables share the same start point, don't free transactions at this case
            return 0;
        }
        return endpoint;
    }

    private static long max(long n1, long n2, long n3) {
        return Math.max(n1, Math.max(n2, n3));
    }
}
