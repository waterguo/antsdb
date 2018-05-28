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

import java.util.Comparator;

/**
 * compare two byte arrays using BigInteger (big endian) format
 * 
 * @author xguo
 *
 */
public class NumericComparator implements Comparator<byte[]> {

    @Override
    public int compare(byte[] o1, byte[] o2) {
        int sign1 = ((o1[0] & 0x80) == 0) ? 1 : -1;
        int sign2 = ((o2[0] & 0x80) == 0) ? 1 : -1;
        int result = sign1 - sign2;
        if (result != 0) {
            return result;
        }
        int len = Math.max(o1.length, o2.length);
        for (int i=0; i<len; i++) {
            int value1 = getByte(o1, sign1, len, i);
            int value2 = getByte(o2, sign2, len, i);
            result = value1 - value2;
            if (result != 0) {
                return result;
            }
        }
        return 0;
    }

    private int getByte(byte[] bytes, int sign, int len, int i) {
        int idx = bytes.length - len + i;
        if (idx < 0) {
            return (sign > 0) ? 0 : -1;
        }
        return bytes[idx];
    }

}
