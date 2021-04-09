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
        if (value.toLowerCase().endsWith("k")) {
            unit = 1024;
            value = value.substring(0, value.length()-1);
        }
        else if (value.toLowerCase().endsWith("m")) {
            unit = 1024*1024;
            value = value.substring(0, value.length()-1);
        }
        else if (value.toLowerCase().endsWith("g")) {
            unit = 1024*1024*1024;
            value = value.substring(0, value.length()-1);
        }
        long result = Long.parseLong(value) * unit;
        return result;
    }
}
