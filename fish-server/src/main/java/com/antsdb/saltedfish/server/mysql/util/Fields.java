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
package com.antsdb.saltedfish.server.mysql.util;

/**
 * https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnType
 */
public interface Fields {

    /** field data type */
    public static final byte FIELD_TYPE_DECIMAL = 0;
    public static final byte FIELD_TYPE_TINY = 1;
    public static final byte FIELD_TYPE_SHORT = 2;
    public static final byte FIELD_TYPE_LONG = 3;
    public static final byte FIELD_TYPE_FLOAT = 4;
    public static final byte FIELD_TYPE_DOUBLE = 5;
    public static final byte FIELD_TYPE_NULL = 6;
    public static final byte FIELD_TYPE_TIMESTAMP = 7;
    public static final byte FIELD_TYPE_LONGLONG = 8;
    public static final byte FIELD_TYPE_INT24 = 9;
    public static final byte FIELD_TYPE_DATE = 10;
    public static final byte FIELD_TYPE_TIME = 11;
    public static final byte FIELD_TYPE_DATETIME = 12;
    public static final byte FIELD_TYPE_YEAR = 13;
    public static final byte FIELD_TYPE_NEWDATE = 14;
    public static final byte FIELD_TYPE_VARCHAR = 15;
    public static final byte FIELD_TYPE_BIT = 16;
    public static final byte FIELD_TYPE_TIMESTAMP2 = 17;
    public static final byte FIELD_TYPE_DATETIME2 = 18;
    public static final byte FIELD_TYPE_TIME2 = 19;
    public static final byte FIELD_TYPE_NEW_DECIMAL = (byte)246;
    public static final byte FIELD_TYPE_ENUM = (byte)247;
    public static final byte FIELD_TYPE_SET = (byte)248;
    public static final byte FIELD_TYPE_TINY_BLOB = (byte)249;
    public static final byte FIELD_TYPE_MEDIUM_BLOB = (byte)250;
    public static final byte FIELD_TYPE_LONG_BLOB = (byte)251;
    public static final byte FIELD_TYPE_BLOB = (byte)252;
    public static final byte FIELD_TYPE_VAR_STRING = (byte)253;
    public static final byte FIELD_TYPE_STRING = (byte)254;
    public static final byte FIELD_TYPE_GEOMETRY = (byte)255;

    /** field flag */
    public static final int NOT_NULL_FLAG = 0x0001;
    public static final int PRI_KEY_FLAG = 0x0002;
    public static final int UNIQUE_KEY_FLAG = 0x0004;
    public static final int MULTIPLE_KEY_FLAG = 0x0008;
    public static final int BLOB_FLAG = 0x0010;
    public static final int UNSIGNED_FLAG = 0x0020;
    public static final int ZEROFILL_FLAG = 0x0040;
    public static final int BINARY_FLAG = 0x0080;
    public static final int ENUM_FLAG = 0x0100;
    public static final int AUTO_INCREMENT_FLAG = 0x0200;
    public static final int TIMESTAMP_FLAG = 0x0400;
    public static final int SET_FLAG = 0x0800;

}