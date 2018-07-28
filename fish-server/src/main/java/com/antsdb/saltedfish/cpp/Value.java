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

public class Value {
    public static final byte TYPE_NULL = 0;
    public static final byte TYPE_NUMBER = 0x10;
    public static final byte TYPE_STRING = 0x30;
    public static final byte TYPE_BOOL = 0x60;
    public static final byte TYPE_DATE = 0x70;
    public static final byte TYPE_TIME = (byte)0x80;
    public static final byte TYPE_TIMESTAMP = (byte)0x90;
    public static final byte TYPE_BYTES = (byte)0xa0;
    public static final byte TYPE_CLOB = (byte)0xb0;
    public static final byte TYPE_BLOB = (byte)0xc0;
    public static final byte TYPE_UNKNOWN = (byte)0xd0;
    public static final byte FORMAT_RECORD = (byte)0xf0;
    
    /** 0xd0 - 0xff are reserved */

    public static final byte FORMAT_NULL = TYPE_NULL;
    /** 4 bytes integer */
    public static final byte FORMAT_INT4 = TYPE_NUMBER + 2;
    /** 8 bytes integer */
    public static final byte FORMAT_INT8 = TYPE_NUMBER + 4;
    /** variable length integer */
    public static final byte FORMAT_BIGINT = TYPE_NUMBER + 6;
    /** decimal number */
    public static final byte FORMAT_FAST_DECIMAL = TYPE_NUMBER + 8;
    /** decimal number */
    public static final byte FORMAT_DECIMAL = TYPE_NUMBER + 9;
    /** 4 bytes float */
    public static final byte FORMAT_FLOAT4 = TYPE_NUMBER + 12;
    /** 8 bytes flat */
    public static final byte FORMAT_FLOAT8 = TYPE_NUMBER + 13;
    /** time */
    public static final byte FORMAT_TIME = TYPE_TIME;
    /** date */
    public static final byte FORMAT_DATE = TYPE_DATE;
    /** datetime */
    public static final byte FORMAT_TIMESTAMP = TYPE_TIMESTAMP;
    /** utf8 string */
    public static final byte FORMAT_UTF8 = TYPE_STRING + 4;
    /** utf16 string */
    public static final byte FORMAT_UNICODE16 = TYPE_STRING + 8;
    /** boolean */
    public static final byte FORMAT_BOOL = TYPE_BOOL;
    /** bytes */
    public static final byte FORMAT_BYTES = TYPE_BYTES;
    /** key bytes */
    public static final byte FORMAT_KEY_BYTES = TYPE_BYTES + 1;
    /** boundary is a combination of key and inclusive indicator */
    public static final byte FORMAT_BOUNDARY = TYPE_UNKNOWN + 0;
    /** an array of integers **/
    public static final byte FORMAT_INT4_ARRAY = TYPE_UNKNOWN + 1;
    /** clob reference */
    public static final byte FORMAT_CLOB_REF = TYPE_UNKNOWN + 2;
    /** blob reference */
    public static final byte FORMAT_BLOB_REF = TYPE_UNKNOWN + 3;
    public static final byte FORMAT_ROW = TYPE_UNKNOWN + 0x10;
    
    public final static byte getFormat(Heap heap, long address) {
        if (address == 0) {
            return 0;
        }
        byte n = Unsafe.getByte(address);
        return n;
    }
    
    public final static byte getType(Heap heap, long addr) {
        if (addr == 0) {
            return 0;
        }
        byte format = getFormat(heap, addr);
        return getType(format);
    }
    
    public final static byte getType(int format) {
        return (byte)(format & 0xf0);
    }

}
