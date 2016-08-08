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
package com.antsdb.saltedfish.server.mysql.packet;

import com.antsdb.saltedfish.server.mysql.MysqlServerHandler;

import io.netty.buffer.ByteBuf;

public class ShutdownPacket extends RecievePacket {

    public ShutdownPacket() {
        super(-1);
    }

    @Override
    public void read(MysqlServerHandler handler, ByteBuf in) {
    }
}
