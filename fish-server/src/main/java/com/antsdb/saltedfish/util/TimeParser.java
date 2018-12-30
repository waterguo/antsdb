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

import org.apache.commons.lang.StringUtils;

/**
 * 
 * @author *-xguo0<@
 */
public final class TimeParser {
    /**
     * parse the string as number of seconds
     * @param value
     * @return
     */
    public static long parseSeconds(String value, String defecto) {
        long result;
        if (StringUtils.isEmpty(value)) {
            value = defecto;
        }
        char suffix = value.charAt(value.length()-1);
        suffix = Character.isDigit(suffix) ? 0 : suffix;
        String number = (suffix != 0) ? value.substring(0, value.length()-1) : value;
        switch (suffix) {
        case 0:
        case 's':
            result = Long.parseLong(number);
            break;
        case 'm':
            result = Long.parseLong(number) * 60;
            break;
        case 'h':
            result = Long.parseLong(number) * 60 * 60;
            break;
        case 'd':
            result = Long.parseLong(number) * 60 * 60 * 24;
            break;
        case 'w':
            result = Long.parseLong(number) * 60 * 60 * 24 * 7;
            break;
        default:
            throw new IllegalArgumentException(value);
        }
        return result;
    }
}
