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
public final class UberFormatter {
    public static String hex(Integer value) {
        if (value == null) {
            return "null";
        }
        return hex((long)value);
    }
    
    public static String hex(Long value) {
        if (value == null) {
            return "null";
        }
        String result = String.format("0x%x", value);
        return result;
    }
    
    public static String time(long value) {
        value = value / 1000;
        long s = value % 60;
        value = value / 60;
        long m = value % 60;
        long h = value / 60;
        String result = String.format("%d:%02d:%02d", h, m, s);
        return result;
    }
    
    public static String capacity(long size) {
        String unit;
        double value;
        if (size < SizeConstants.KB) {
            unit = "bytes";
            value = size;
        }
        else if (size < SizeConstants.MB) {
            unit = "KB";
            value = size / (double)SizeConstants.KB;
        }
        else if (size < SizeConstants.GB) {
            unit = "MB";
            value = size / (double)SizeConstants.MB;
        }
        else if (size < SizeConstants.TB) {
            unit = "GB";
            value = size / (double)SizeConstants.GB;
        }
        else {
            unit = "TB";
            value = size / (double)SizeConstants.TB;
        }
        String result = String.format("%.2f %s", value, unit);
        return result;
    }
}
