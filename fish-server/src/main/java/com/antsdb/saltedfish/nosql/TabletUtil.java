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

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import com.antsdb.saltedfish.util.LongLong;

/**
 * 
 * @author *-xguo0<@
 */
public class TabletUtil {
    public static MemTablet findOldestActiveTablet(Humpback humpback, long start) {
        long sp = Long.MAX_VALUE;
        MemTablet result = null;
        for (GTable i:humpback.getTables()) {
            for (MemTablet j:i.memtable.getTablets()) {
                if (j.isCarbonfrozen()) {
                    continue;
                }
                LongLong span = j.getLogSpan();
                if (span == null) {
                    continue;
                }
                if (span.x < sp) {
                    sp = span.x;
                    result = j;
                }
            }
        }
        return result;
    }
    
    public static MemTablet findOldestTablet(Humpback humpback, long start, boolean onlyCarbonfrozen) {
        AtomicReference<MemTablet> result = new AtomicReference<>();
        LongLong oldest = new LongLong(Long.MAX_VALUE, Long.MAX_VALUE);
        walkAllTablets(humpback, tablet -> {
            if (onlyCarbonfrozen && !tablet.isCarbonfrozen()) {
                return;
            }
            LongLong span = tablet.getLogSpan();
            if (span == null) {
                return;
            }
            if (span.x < start) {
                return;
            }
            if (span.x >= oldest.x) {
                return;
            }
            result.set(tablet);
            oldest.x = span.x;
            oldest.y = span.y;
        });
        return result.get();
    }

    public static void walkAllTablets(Humpback humpback, Consumer<MemTablet> consumer) {
        for (GTable i:humpback.getTables()) {
            for (MemTablet j:i.memtable.getTablets()) {
                consumer.accept(j);
            }
        }
    }
    
    /**
     * get the bytes hold in the log by the tablet
     * 
     * @return
     */
    public static long getHoldDataSize(MemTablet tablet) {
        LongLong span = tablet.getLogSpan();
        if (span == null) {
            return 0;
        }
        SpaceManager sm = tablet.humpback.getSpaceManager();
        long result = sm.minus(sm.getAllocationPointer(), span.x);
        return result;
    }
}
