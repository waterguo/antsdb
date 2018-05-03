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

/**
 * 
 * @author *-xguo0<@
 */
public final class Size {
    public static long parse(String value, String defaultValue) {
        if (value == null) {
            value = defaultValue;
        }
        value = value.toLowerCase();
        long unit = 1;
        if (value.endsWith("k")) {
            unit = 1024;
            value = value.substring(0, value.length()-1);
        }
        else if (value.endsWith("m")) {
            unit = 1024*1024;
            value = value.substring(0, value.length()-1);
        }
        else if (value.endsWith("g")) {
            unit = 1024*1024*1024;
            value = value.substring(0, value.length()-1);
        }
        long result = Long.parseLong(value) * unit;
        return result;
    }
}
