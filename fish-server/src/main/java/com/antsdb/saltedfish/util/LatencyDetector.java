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
package com.antsdb.saltedfish.util;

import java.util.function.Supplier;

import org.slf4j.Logger;

/**
 * 
 * @author *-xguo0<@
 */
public final class LatencyDetector {
    private static int _threshold;
    
    public static void set(int threshold) {
        _threshold = threshold;
    }
    
    public static <T> T run(Logger log, String info, Supplier<T> callback) {
        if (_threshold <= 0) {
            return callback.get();
        }
        else {
            long start = UberTime.getTime();
            T result = callback.get();
            long end = UberTime.getTime();
            long elapsed = end - start; 
            if (elapsed >= _threshold) {
                log.debug("latency-{} {}", info, elapsed);
            }
            return result;
        }
    }
}
