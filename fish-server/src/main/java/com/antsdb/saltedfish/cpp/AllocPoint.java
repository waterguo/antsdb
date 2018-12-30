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
 * @author *-xguo0<@
 */
public final class AllocPoint {
    public static final int LISTENER = 0;
    public static final int BIG_PACKET = 1;
    public static final int HBASE_READ_BUFFER = 2;
    public static final int MYSQL_CLIENT = 3;
    public static final int PACKET_WRITER = 4;
    public static final int CHANNEL_WRITER = 5;
    public static final int ASYNCHRONOUS_INSERT = 6;
    public static final int END = 7;
}
