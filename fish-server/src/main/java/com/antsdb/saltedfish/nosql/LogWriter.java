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

import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;

import com.antsdb.saltedfish.util.UberUtil;

/**
 * WAL writer, works together with Gobbler
 *  
 * @author wgu0
 */
class LogWriter extends Thread {
    static Logger _log = UberUtil.getThisLogger();
    static final long ALIGNMENT = 16 * 1024;

    private SpaceManager spaceman;
    private Gobbler gobbler;
    private boolean isClosed = false;
    
    public LogWriter(SpaceManager spaceman, Gobbler gobbler) {
        super(LogWriter.class.getSimpleName());
        setDaemon(true);
        this.spaceman = spaceman;
        this.gobbler = gobbler;
    }
    
    @Override
    public void run() {
        for (;;) {
            if (this.isClosed) {
                break;
            }
            try {
                run_();
            }
            catch (Exception x) {
                _log.error("error", x);
            }
        }
        _log.info("log writer closed");
    }

    private void run_() {
        for (;;) {
            // is there anything to write?
            
            AtomicLong atomPersistence = this.gobbler.getPersistencePointer();
            long spPersistence = atomPersistence.get();
            long spAllocation = this.spaceman.getAllocationPointer();
            if (spPersistence >= spAllocation) {
                if (this.isClosed) {
                    return;
                }
                UberUtil.sleep(1);
                continue;
            }
            
            // mark the end point

            long delta = spAllocation - spPersistence;
            if (delta >= ALIGNMENT) {
                long aligned = spAllocation / ALIGNMENT * ALIGNMENT;
                if (aligned > spPersistence) {
                    spAllocation = aligned;
                }
            }
            atomPersistence.set(spAllocation);
            
            // going to write
            
            this.spaceman.force(spPersistence, spAllocation);
        }
    }

    public void close() {
        _log.info("shutting down log writer ... ");
        this.isClosed = true;
    }

}
