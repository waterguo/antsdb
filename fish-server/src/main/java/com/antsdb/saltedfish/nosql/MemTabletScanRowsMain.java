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

import java.io.File;

import com.antsdb.saltedfish.cpp.BetterCommandLine;
import com.antsdb.saltedfish.util.UberFormatter;

/**
 * scan all rows from a tablet to measure performance
 * 
 * @author *-xguo0<@
 */
public class MemTabletScanRowsMain extends BetterCommandLine {

    @Override
    protected String getCommandName() {
        return "tablet-scan-rows <antsdb home> <tablet>";
    }

    public static void main(String[] args) throws Exception {
        new MemTabletScanRowsMain().parseAndRun(args);
    }

    @Override
    protected void run() throws Exception {
        if (this.cmdline.getArgs().length != 2) {
            println("error: invalid argument");
            return;
        }
        File home = new File(this.cmdline.getArgs()[0]);
        File file = new File(this.cmdline.getArgs()[1]);
        SpaceManager sm = new SpaceManager(home);
        sm.open();
        MemTablet tablet = new MemTablet(file);
        tablet.setMutable(false);
        tablet.open();
        tablet.setSpaceManager(sm);
        MemTablet.Scanner sr = tablet.scanDelta(0, Long.MAX_VALUE);
        long start = System.currentTimeMillis();
        int count = 0;
        long checksum = 0;
        long minsp = Long.MAX_VALUE;
        long maxsp = 0;
        while (sr.next()) {
            long sp = sr.getLogSpacePointer();
            minsp = Math.min(minsp, sp);
            maxsp = Math.max(maxsp, sp);
            Row row = sr.getRow();
            checksum = checksum ^ row.getHash();
            count++;
        }
        long end = System.currentTimeMillis();
        long duration = (end - start) / 1000;
        long speed = (duration == 0) ? count : count / duration;
        println("count=%d duration=%d throughput=%d", count, duration, speed);
        println("minsp=%s maxsp=%s", UberFormatter.hex(minsp), UberFormatter.hex(maxsp));
        tablet.close();
    }

}
