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
package com.antsdb.saltedfish.sql;

/**
 * used to decide the up cast of data types
 * @author *-xguo0<@
 */
public class Weight {
    public final static int CAT_CHAR = 0x00;
    public final static int CAT_BINARY = 0x100;
    public final static int FAMILY_NUMBER = CAT_CHAR | 0x10;
    public final static int FAMILY_CHAR = CAT_CHAR | 0x20;
    public final static int FAMILY_TIME = CAT_CHAR | 0x30;
    public final static int FAMILY_BINARY = CAT_BINARY;
    public final static int OTHER = FAMILY_CHAR;
    public final static int INT1 = FAMILY_NUMBER | 1;
    public final static int INT2 = FAMILY_NUMBER | 2;
    public final static int INT4 = FAMILY_NUMBER | 3;
    public final static int INT8 = FAMILY_NUMBER | 4;
    public final static int DECIMAL = FAMILY_NUMBER | 5;
    public final static int FLOAT = FAMILY_NUMBER | 6;
    public final static int DOUBLE = FAMILY_NUMBER | 7;
    public final static int TIME = FAMILY_TIME | 0;
    public final static int DATE = FAMILY_TIME | 1;
    public final static int DATETIME = FAMILY_TIME | 2;
    public final static int CHAR = FAMILY_CHAR | 0;
    public final static int VARCHAR = FAMILY_CHAR | 1;
    public final static int CLOB = FAMILY_CHAR | 2;
    public final static int BINARY = FAMILY_BINARY | 0;
    public final static int VARBINARY = FAMILY_BINARY | 1;
    public final static int BLOB = FAMILY_BINARY | 2;
    
    public static int getFamily(int x) {
        return x & 0xfffffff0;
    }
}
