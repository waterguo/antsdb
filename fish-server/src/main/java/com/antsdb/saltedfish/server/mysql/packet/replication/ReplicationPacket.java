/*-------------------------------------------------------------------------------------------------
 _______ __   _ _______ _______ ______  ______
 |_____| | \  |    |    |______ |     \ |_____]
 |     | |  \_|    |    ______| |_____/ |_____]

 Copyright (c) 2016, antsdb.com and/or its affiliates. All rights reserved. *-xguo0<@

 This program is free software: you can redistribute it and/or modify it under the terms of the
 GNU Affero General Public License, version 3, as published by the Free Software Foundation.

 You should have received a copy of the GNU Affero General Public License along with this program.
 If not, see <https://www.gnu.org/licenses/agpl-3.0.txt>
-------------------------------------------------------------------------------------------------*/
package com.antsdb.saltedfish.server.mysql.packet.replication;

import com.antsdb.saltedfish.server.mysql.MysqlClientHandler;
import com.antsdb.saltedfish.util.UberUtil;

import io.netty.buffer.ByteBuf;

/**
 * @author roger
 */
public abstract class ReplicationPacket {
    public static int packetHeaderSize = 4;
    // 19 for V4 format
    public static int eventHeaderSize = 19;
    /**
     * start event
     */
    public static final byte START_EVENT_V3 = 1;

    /**
     * query event
     */
    public static final byte QUERY_EVENT = 2;

    /**
     * stop event
     */
    public static final byte STOP_EVENT = 3;

    /**
     * rotate event
     */
    public static final byte ROTATE_EVENT = 4;

    /**
     * format event
     */
    public static final byte FORMAT_DESCRIPTION_EVENT = 15;

    /**
     * xid event
     */
    public static final byte XID_EVENT = 16;

    /**
     * table map event
     */
    public static final byte TABLE_MAP_EVENT = 19;

    /**
     * write rows event v2
     */
    public static final byte WRITE_ROWS_EVENT = 30;

    /**
     * update rows event v2
     */
    public static final byte UPDATE_ROWS_EVENT = 31;

    /**
     * delete rows event v2
     */
    public static final byte DELETE_ROWS_EVENT = 32;

    public int packetLength;
    public byte packetId;

    public int eventType;
    public long eventlength;
    public long nextPosition;

    public ReplicationPacket(int type, long length, long pos) {
    	eventType = type;
    	eventlength = length;
    	nextPosition = pos;
    }

    public ReplicationPacket(int type) {
    	eventType = type;
    }

    public abstract void read(MysqlClientHandler handler, ByteBuf in) ;

    @Override
    public String toString() {
        return UberUtil.toString(this);
    }

}