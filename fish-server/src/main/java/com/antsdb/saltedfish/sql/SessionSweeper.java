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
import com.antsdb.saltedfish.util.UberUtil;

/**
 * periodically scan sessions to determine oldest transaction and remove expired sessions
 * 
 * @author wgu0
 */
public class SessionSweeper implements Runnable {
    static Logger _log = UberUtil.getThisLogger();

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
                humpback.setLastClosedTransactionId(lastClosed);
                humpback.getGobbler().logTransactionWindow(lastClosed);
                _log.trace("session sweeper found last trx: {}", lastClosed);
            }
        }
        catch (Exception x) {
            _log.error("unexpected exception", x);
        }
    }

}
