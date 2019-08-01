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
public final class TypeId {
    public static final byte NULL = 0;
    public static final byte BOOL = 1;
    public static final byte BYTE = 4;
    public static final byte SHORT = 8;
    public static final byte INT = 10;
    public static final byte FLOAT = 11;
    public static final byte LONG = 20;
    public static final byte DOUBLE = 21;
    public static final byte TIMESTAMP = 22;
    public static final byte STRING = 30;
    public static final byte BINARY = 31;
    public static final byte DECIAML = 32;
    public static final byte UNKNOWN = -1;
}
