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
public final class SizeConstants {
    public static int KB = 1024;
    public static int MB = 1024 * KB;
    public static int GB = 1024 * MB;
    public static long TB = 1024l * GB;
    
    public static int kb(int value) {
        return KB * value;
    }

    public static long mb(int value) {
        return MB * value;
    }

    public static long gb(long value) {
        return GB * value;
    }
}
