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
package com.antsdb.saltedfish.util;

import org.apache.commons.lang.StringUtils;

/**
 * 
 * @author *-xguo0<@
 */
public class ParseUtil {
    public static int parseInteger(String value, int defaultValue) {
        Integer result = parseInteger(value);
        return (result == null) ? defaultValue : result;
    }
    
    public static Integer parseInteger(String value) {
        if (value == null) {
            return null;
        }
        int result = 0;
        if (value.startsWith("0x")) {
            value = StringUtils.substring(value, 2);
            result = Integer.parseInt(value, 16);
        }
        else {
            result = Integer.parseInt(value);
        }
        return result;
    }

    public static long parseLong(String value, long defaultValue) {
        Long result = parseLong(value);
        return (result == null) ? defaultValue : result;
    }
    
    public static Long parseLong(String value) {
        if (value == null) {
            return null;
        }
        long result = 0;
        if (value.startsWith("0x")) {
            value = StringUtils.substring(value, 2);
            result = Long.parseLong(value, 16);
        }
        else {
            result = Long.parseLong(value);
        }
        return result;
    }
    
    public static boolean parseBoolean(String value, Boolean defaultValue) {
        Boolean result = parseBoolean(value);
        return (result == null) ? defaultValue : result;
    }
    
    public static Boolean parseBoolean(String value) {
        if (value == null) {
            return null;
        }
        Boolean result = Boolean.getBoolean(value);
        return result;
    }
}
