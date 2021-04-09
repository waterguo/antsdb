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

/**
 * 
 * @author *-xguo0<@
 */
public final class ScanOptions {
    public static final long EXCLUDE_START = 0x1;
    public static final long EXCLUDE_END = 0x2;
    public static final long DESCENDING = 0x4;
    public static final long NO_CACHE = 0x8;
    public static final long SHOW_DELETE_MARK = 0x10;

    public static long excludeStart(long options) {
        return options | EXCLUDE_START;
    }
    
    public static long excludeEnd(long options) {
        return options | EXCLUDE_END;
    }

    public static long descending(long options) {
        return options | DESCENDING;
    }
    
    public static long noCache(long options) {
        return options | NO_CACHE;
    }

    public static boolean hasNoCache(long options) {
        return (options & NO_CACHE) != 0;
    }
    
    public static boolean has(long value, long flag) {
        return (value & flag) != 0;
    }
    
    public static boolean includeStart(long value) {
        return !has(value, EXCLUDE_START);
    }
    
    public static boolean includeEnd(long value) {
        return !has(value, EXCLUDE_END);
    }
    
    public static boolean isAscending(long value) {
        return !has(value, DESCENDING);
    }
    
    public static boolean isShowDeleteMarkOn(long value) {
        return (value & SHOW_DELETE_MARK) != 0;
    }
}
