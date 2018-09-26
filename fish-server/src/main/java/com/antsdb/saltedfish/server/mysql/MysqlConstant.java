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
package com.antsdb.saltedfish.server.mysql;

/**
 * 
 * @author wgu0
 */
public interface MysqlConstant {
    public static final int    CLIENT_LONG_PASSWORD        = 0x00000001; /* new more secure passwords */
    public static final int    CLIENT_FOUND_ROWS            = 0x00000002;
    public static final int    CLIENT_LONG_FLAG            = 0x00000004; /* Get all column flags */
    public static final int    CLIENT_CONNECT_WITH_DB        = 0x00000008;
    public static final int    CLIENT_COMPRESS                = 0x00000020; /* Can use compression protcol */
    public static final int    CLIENT_LOCAL_FILES            = 0x00000080; /* Can use LOAD DATA LOCAL */
    public static final int    CLIENT_PROTOCOL_41            = 0x00000200; // for > 4.1.1
    public static final int    CLIENT_INTERACTIVE            = 0x00000400;
    public static final int    CLIENT_SSL                    = 0x00000800;
    public static final int    CLIENT_TRANSACTIONS            = 0x00002000; // Client knows about transactions
    public static final int    CLIENT_RESERVED                = 0x00004000; // for 4.1.0 only
    public static final int    CLIENT_SECURE_CONNECTION    = 0x00008000;
    public static final int    CLIENT_MULTI_STATEMENTS        = 0x00010000; // Enable/disable multiquery support
    public static final int    CLIENT_MULTI_RESULTS        = 0x00020000; // Enable/disable multi-results
    public static final int    CLIENT_PLUGIN_AUTH            = 0x00080000;
    public static final int    CLIENT_CONNECT_ATTRS        = 0x00100000;
    public static final int    CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA    = 0x00200000;
    public static final int    CLIENT_CAN_HANDLE_EXPIRED_PASSWORD        = 0x00400000;
    public static final int CLIENT_ODBC                 = 0x00000040;
    public static final int MYSQL_COLLATION_INDEX_latin1_swedish_ci = 8;
    public static final int MYSQL_COLLATION_INDEX_utf8_general_ci = 33;
    public static final int MYSQL_COLLATION_INDEX_utf8mb_general_ci = 45;
    public static final int MYSQL_COLLATION_INDEX_binary = 63;
    public static final int MYSQL_COLLATION_INDEX_utf8_bin = 83;

    // mysql charset index see com.mysql.jdbc.CharsetMapping
    public static final int MYSQL_CHARSET_NAME_binary = 63;
    public static final int MYSQL_CHARSET_NAME_utf8 = 83;
    
    public static final int SERVER_STATUS_IN_TRANS = 1;
    public static final int SERVER_STATUS_AUTOCOMMIT = 2; // Server in auto_commit mode

    // Data Types
    static final int FIELD_TYPE_DECIMAL = 0;
    static final int FIELD_TYPE_DOUBLE = 5;
    static final int FIELD_TYPE_ENUM = 247;
    static final int FIELD_TYPE_FLOAT = 4;
    static final int FIELD_TYPE_GEOMETRY = 255;
    static final int FIELD_TYPE_INT24 = 9;
    static final int FIELD_TYPE_LONG = 3;
    static final int FIELD_TYPE_LONG_BLOB = 251;
    static final int FIELD_TYPE_LONGLONG = 8;
    static final int FIELD_TYPE_MEDIUM_BLOB = 250;
    static final int FIELD_TYPE_NEW_DECIMAL = 246;
    static final int FIELD_TYPE_NEWDATE = 14;
    static final int FIELD_TYPE_NULL = 6;
    static final int FIELD_TYPE_SET = 248;
    static final int FIELD_TYPE_SHORT = 2;
    static final int FIELD_TYPE_STRING = 254;
    static final int FIELD_TYPE_TIME = 11;
    static final int FIELD_TYPE_TIMESTAMP = 7;
    static final int FIELD_TYPE_TINY = 1;
    static final int FIELD_TYPE_TINY_BLOB = 249;
    static final int FIELD_TYPE_VAR_STRING = 253;
    static final int FIELD_TYPE_VARCHAR = 15;
    static final int FIELD_TYPE_YEAR = 13;
    static final int FIELD_TYPE_BIT = 16;
    static final int FIELD_TYPE_BLOB = 252;
    static final int FIELD_TYPE_DATE = 10;
    static final int FIELD_TYPE_DATETIME = 12;
    
    // data type flags
    
    /* The field value cannot be NULL (it is declared with the NOT NULL attribute). */
    static final int NOT_NULL_FLAG = 0x1;
    /* The field is a part of the primary key. */
    static final int PRI_KEY_FLAG = 0x2;
    /* The field is a part of a unique key. */
    static final int UNIQUE_KEY_FLAG = 0x0004;
    /* The field is a part of some non-unique key. */
    static final int  MULTIPLE_KEY_FLAG = 0x0008;
    /* The field is a BLOB or TEXT. */
    static final int BLOB_FLAG = 0x0010;
    /* The field was declared with the UNSIGNED attribute, which has the same meaning as the unsigned keyword in C. */
    static final int UNSIGNED_FLAG = 0x0020;
    /* The field has been declared with the ZEROFILL attribute, which tells the server to pad the numeric types with
     *  leading zeros in the output to fit the specified field length.
     */
    static final int ZEROFILL_FLAG = 0x0040;
    /* The field has been declared with the BINARY attribute, which tells the server to compare strings byte-for-byte
     *  in a case-sensitive manner.
     */
    static final int BINARY_FLAG = 0x0080;
    /* The field is an ENUM. */
    static final int ENUM_FLAG = 0x0100;
    /* The field has been declared with the AUTO_INCREMENT attribute, which enables the automatic generation of 
     * primary key values when a new record is inserted.
     */
    static final int AUTO_INCREMENT_FLAG = 0x0200;
    /* The field is a timestamp. */
    static final int TIMESTAMP_FLAG = 0x0400;
    /* The field is a SET. */
    static final int SET_FLAG = 0x0800;
    /* Used with cursors in version 4.1 to indicate that the field is numeric. */
    static final int NUM_FLAG = 0x8000;
}