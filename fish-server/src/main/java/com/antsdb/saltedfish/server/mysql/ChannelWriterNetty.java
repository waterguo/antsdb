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
package com.antsdb.saltedfish.server.mysql;

import java.nio.ByteBuffer;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;

/**
 * 
 * @author *-xguo0<@
 */
public class ChannelWriterNetty extends ChannelWriter {
    private Channel ch;

    public ChannelWriterNetty(Channel ch) {
        this.ch = ch;
    }
    
    @Override
    protected void writeDirect(byte[] bytes) {
        this.ch.writeAndFlush(Unpooled.copiedBuffer(bytes));
    }

    @Override
    protected void writeDirect(ByteBuffer bytes) {
        this.ch.writeAndFlush(Unpooled.copiedBuffer(bytes));
    }

}
