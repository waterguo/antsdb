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

import java.util.function.IntSupplier;

/**
 * 
 * umbrella class for all string like data
 * 
 * @author xinyi
 *
 */
public final class FishString {
    public final static boolean isString(long addr) {
        if (addr == 0) {
            return false;
        }
        int type = Value.getType(null, addr);
        return type == Value.TYPE_STRING;
    }
    
    final static boolean isString_(long addr) {
        int type = Unsafe.getByte(addr);
        switch (type) {
        case Value.FORMAT_UNICODE16:
            return true;
        default:
            return false;
        }
    }
    
    public final static boolean isEmpty(long addr) {
        if (addr == 0) {
            return false;
        }
        int format = Value.getFormat(null, addr);
        if (format == Value.FORMAT_UNICODE16) {
            return Unicode16.getLength(format, addr) == 0;
        }
        else if (format == Value.FORMAT_UTF8) {
            return FishUtf8.getStringSize(Value.FORMAT_UTF8, addr) == 0;
        }
        else {
            throw new IllegalArgumentException();
        }
    }

    final static boolean equals(long xAddr, long yAddr) {
        return compare(xAddr, yAddr) == 0;
    }

    public static final IntSupplier scan(long addr) {
        int format = Value.getFormat(null, addr);
        if (format == Value.FORMAT_UNICODE16) {
            return new Unicode16(addr).scan();
        }
        else if (format == Value.FORMAT_UTF8) {
            return new FishUtf8(addr).scan();
        }
        else {
            throw new IllegalArgumentException();
        }
    }
    
    public final static int compare(long xAddr, long yAddr) {
        IntSupplier scanx = scan(xAddr);
        IntSupplier scany = scan(yAddr);
        for (;;) {
            int chx = scanx.getAsInt();
            int chy = scany.getAsInt();
            if ((chx == -1) && (chy == -1)) {
                break;
            }
            int result = chx - chy;
            if (result != 0) {
                return result;
            }
        }
        return 0;
    }
    
    public final static int getEsitimatedLength(long addr) {
        int result;
        int format = Value.getFormat(null, addr);
        if (format == Value.FORMAT_UNICODE16) {
            result = new Unicode16(addr).getLength();
        }
        else if (format == Value.FORMAT_UTF8) {
            result = new FishUtf8(addr).getSize();
        }
        else {
            throw new IllegalArgumentException();
        }
        return result;
    }

    public final static long concat(Heap heap, long addrX, long addrY) {
        if (addrX == 0) {
            return addrY;
        }
        if (addrY == 0) {
            return addrX;
        }
        int length = getEsitimatedLength(addrX) + getEsitimatedLength(addrY);
        Unicode16 result = new Unicode16(Unicode16.allocSet(heap, length));
        int i=0;
        for (IntSupplier scan=scan(addrX);;) {
            int ch = scan.getAsInt();
            if (ch < 0) {
                break;
            }
            result.put(i++, (char)ch);
        }
        for (IntSupplier scan=scan(addrY);;) {
            int ch = scan.getAsInt();
            if (ch < 0) {
                break;
            }
            result.put(i++, (char)ch);
        }
        result.resize(i);
        return result.getAddress();
    }

    public static int getLength(long pValue) {
        if (pValue == 0) {
            return 0;
        }
        int format = Value.getFormat(null, pValue);
        int len;
        if (format == Value.FORMAT_UNICODE16) {
            len = Unicode16.getLength(format, pValue);
        }
        else if (format == Value.FORMAT_UTF8){
            len = FishUtf8.getLength(format, pValue);
        }
        else {
            throw new IllegalArgumentException();
        }
        return len;
    }

    public static char charAt(int format, int length, long pValue, int idx) {
        if (format == Value.FORMAT_UNICODE16) {
            return Unicode16.getCharAt(format, length, pValue, idx);
        }
        else {
            throw new IllegalArgumentException();
        }
    }
}
