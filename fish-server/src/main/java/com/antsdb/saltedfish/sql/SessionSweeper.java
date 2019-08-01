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

import org.slf4j.Logger;

import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.util.TimeConstants;
import com.antsdb.saltedfish.util.UberTime;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * periodically scan sessions to determine oldest transaction and remove expired sessions
 * 
 * @author wgu0
 */
public class SessionSweeper implements Runnable {
    static Logger _log = UberUtil.getThisLogger();
    
    long lastWindowTrxId;
    long lastWindowTime;
    Orca orca;
    
    SessionSweeper(Orca orca) {
        this.orca = orca;
    }
    
    @Override
    public synchronized void run() {
        try {
            long lastClosed = this.orca.getLastClosedTransactionId();
            Humpback humpback = this.orca.getHumpback();
            if (lastClosed < humpback.getLastClosedTransactionId()) {
                long now = UberTime.getTime();
                // log trx window when there are enough transactions or long enough
                if ((lastClosed - lastWindowTrxId >= 1000) || (now - lastWindowTime >= TimeConstants.minute(5))) {
                    humpback.setLastClosedTransactionId(lastClosed);
                    humpback.getGobbler().logTransactionWindow(lastClosed);
                    this.lastWindowTime = now;
                    this.lastWindowTrxId = lastClosed;
                    _log.trace("session sweeper found last trx: {}", lastClosed);
                }
            }
        }
        catch (Exception x) {
            _log.error("unexpected exception", x);
        }
    }

}
