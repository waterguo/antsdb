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

import java.util.List;

import org.slf4j.Logger;

import com.antsdb.saltedfish.server.mysql.util.BufferUtils;
import com.antsdb.saltedfish.util.UberUtil;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

/**
 * 
 * @author wgu0
 */
public class MysqlPacketDecoder extends ByteToMessageDecoder {
    static Logger _log = UberUtil.getThisLogger();

	@SuppressWarnings("unused")
	@Override
	protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // do we have length field in buffer ?
        
        if (!in.isReadable(4)) {
            return;
        }

        // do we have entire packet
        
        int pos = in.readerIndex();
        int size = BufferUtils.readLongInt(in);
        int sequence = in.readByte() & 0xff;
        if (!in.isReadable(size)) {
            in.readerIndex(pos);
            return;
        }

        // yup we are ready proceed with handler 
        
        ByteBuf packet = in.slice(pos, size + 4);
        packet.retain();
        try {
	        in.readerIndex(pos + size + 4);
	        _log.trace("packet {}", ByteBufUtil.dump(packet));
	        out.add(packet);
        }
        finally {
        }
	}
}
