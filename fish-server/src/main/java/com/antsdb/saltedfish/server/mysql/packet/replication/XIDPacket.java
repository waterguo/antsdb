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
import com.antsdb.saltedfish.server.mysql.util.BufferUtils;

import io.netty.buffer.ByteBuf;

public class XIDPacket extends ReplicationPacket {
    public long xid;
    
    public XIDPacket(int type, long length, long pos) {
        super(type, length, pos);
    }

    @Override
    public void read(MysqlClientHandler handler, ByteBuf in) {
    	xid = BufferUtils.readLongLong(in);
    }

}
