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
package com.antsdb.mysql.network;

import com.antsdb.saltedfish.nosql.LogEntry;
import com.antsdb.saltedfish.server.mysql.PacketWriter;
import com.antsdb.saltedfish.server.mysql.packet.PacketType;

/**
 * 
 * @author *-xguo0<@
 */
public final class LogReplicateRequest extends MysqlRequest {

    private long pLogEntry;
    private long lpLogEntry;

    public LogReplicateRequest(long lpLogEntry, long pLogEntry) {
        this.pLogEntry = pLogEntry;
        this.lpLogEntry = lpLogEntry;
    }

    public LogReplicateRequest(LogEntry entry) {
        this(entry.getSpacePointer(), entry.getAddress());
    }

    @Override
    public void write(PacketWriter buf) {
        LogEntry entry = new LogEntry(this.lpLogEntry, this.pLogEntry);
        int size = entry.getSize();
        size = LogEntry.HEADER_SIZE + size;
        buf.writeByte((byte)PacketType.FISH_LOG_REPLICATE.getId());
        buf.writeLongLong(this.lpLogEntry);
        buf.writeBytes(this.pLogEntry, size);
    }

}
