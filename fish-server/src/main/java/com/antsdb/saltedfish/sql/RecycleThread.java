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

import com.antsdb.saltedfish.sql.vdm.Measure;
import com.antsdb.saltedfish.sql.vdm.Script;
import com.antsdb.saltedfish.util.UberUtil;

public class RecycleThread extends Thread {
    public static final long RECYLE_INTERVAL_SECONDS = 30;
    
    static Logger _log = UberUtil.getThisLogger();
    
    Orca orca;
    
    public RecycleThread(Orca orca) {
        this.orca = orca;
        setName("fish recyler");
        setDaemon(true);
    }

    @Override
    public void run() {
        for (;;) {
            long start = System.nanoTime();
            try {
                resetMetrics();
            }
            catch (Exception x) {
                _log.error("error", x);
            }
            long end = System.nanoTime();
            long sleep = RECYLE_INTERVAL_SECONDS * 1000 - (end - start) / 1000;
            if (sleep <= 0) {
                continue;
            }
            try {
                Thread.sleep(sleep);
            }
            catch (InterruptedException e) {
            }
        }
    }

    private void resetMetrics() {
        for (Script i:this.orca.statementCache.asMap().values()) {
            Measure m = i.getMeasure();
            if (m == null) {
                continue;
            }
            if (m.isEnabled()) {
                m.recycle();
            }
        }
    }

}
