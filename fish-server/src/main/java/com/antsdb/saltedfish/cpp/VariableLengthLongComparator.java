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
package com.antsdb.saltedfish.cpp;

/**
 * 
 * @author wgu0
 */
public final class VariableLengthLongComparator extends KeyComparator{
    
    @Override
    public int compare(long xAddr, long yAddr) {
        return compare_(xAddr, yAddr);
    }
    
    public static int compare_(long xAddr, long yAddr) {
        if ((xAddr < 10) || (yAddr < 10)) {
            throw new IllegalArgumentException();
        }
        int xLength = KeyBytes.getUnmaskedLength(xAddr);
        int yLength = KeyBytes.getUnmaskedLength(yAddr);
        int minLength = Math.min(xLength, yLength);
        for (int i=0; i<minLength; i+=8) {
            long x = Unsafe.getLongVolatile(xAddr + 4 + i);
            long y = Unsafe.getLongVolatile(yAddr + 4 + i);
            int result = Long.compareUnsigned(x, y);
            if (result != 0) {
                return result;
            }
        }
        return Integer.compare(xLength & 0x7fff, yLength & 0x7fff);
    }
}
