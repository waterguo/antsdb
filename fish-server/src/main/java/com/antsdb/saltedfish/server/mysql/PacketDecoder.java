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
package com.antsdb.saltedfish.server.mysql;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

class PacketDecoder extends ByteToMessageDecoder{
    static final int MAX_PACKET_SIZE = 0xffffff;  
    static final int COMMAND_HANDSKAE= -1; // mysql doesn't have this code
    
    public PacketDecoder() {
        super();
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // do we have length field in buffer ?
        
        if (!in.isReadable(4)) {
            return;
        }

        // do we have entire packet in the buffer?
        
        int readerIndex = in.readerIndex(); 
        int size = in.getIntLE(readerIndex) & 0xffffff;
        if (in.readableBytes() < size + 4) {
            return;
        }
        
        // continue 
        
        in.readInt();
        out.add(in);
    }

}
