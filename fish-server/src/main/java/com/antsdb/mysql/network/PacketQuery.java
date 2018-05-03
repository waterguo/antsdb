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

package com.antsdb.mysql.network;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;

import com.antsdb.saltedfish.server.mysql.packet.PacketType;
import com.antsdb.saltedfish.util.UberUtil;

import io.netty.buffer.ByteBuf;

/**
 * 
 * @author wgu0
 */
public class PacketQuery extends Packet {
    public PacketQuery(ByteBuf buf) {
        this(buf.memoryAddress(), buf.readableBytes());
    }
    
    public PacketQuery(long addr, int length) {
        super(addr, length, PacketType.COM_QUERY);
    }
    
    public PacketQuery(ByteBuffer packet) {
        this(UberUtil.getAddress(packet), packet.remaining());
    }

    public String getQuery() {
        String sql = PacketUtil.readString(addr + 5, this.length - 1);
        return sql;
    }

    public CharBuffer getQueryAsCharBuf() {
        CharBuffer result = PacketUtil.readStringAsCharBufWithMysqlExtension(addr + 5, this.length - 1);
        result.flip();
        return result;
    }
    
    @Override
    public String diffDump(int level) {
        return getQuery();
    }
}
