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
package com.antsdb.saltedfish.server.mysql.packet;

import com.antsdb.saltedfish.server.mysql.MysqlServerHandler;
import com.antsdb.saltedfish.server.mysql.util.BufferUtils;

import io.netty.buffer.ByteBuf;

public class FieldListPacket extends RecievePacket {
    public String table;
    public String fieldWildcard;
    
    public FieldListPacket(int command) {
        super(command);
    }

    @Override
    public void read(MysqlServerHandler handler, ByteBuf in) {
        table = BufferUtils.readString(in);
        fieldWildcard = BufferUtils.readString(in);
    }

}
