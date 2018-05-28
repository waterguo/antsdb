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

public class ByteArrayUtil {
    public static void add(byte[] bytes, byte n) {
        int overflow = 0;
        for (int i = bytes.length-1; i >= 0; i--) {
            int v = (bytes[i] & 0xff) + n + overflow;
            bytes[i] = (byte) v;
            overflow = v >>> 8;
            if (overflow == 0) {
                break;
            }
            n = 0;
        }
        if (overflow != 0) {
            throw new ArithmeticException();
        }
    }
}
