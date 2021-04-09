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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class ChannelWriterNio extends ChannelWriter {

    private SocketChannel channel;

    public ChannelWriterNio(SocketChannel channel) {
        this.channel = channel;
    }
    
    @Override
    protected void writeDirect(byte[] bytes) throws IOException {
        writeDirect(ByteBuffer.wrap(bytes));
    }

    @Override
    protected void writeDirect(ByteBuffer bytes) throws IOException {
        while (bytes.hasRemaining()) {
            this.channel.write(bytes);
        }
    }

    @Override
    public void close() {
        UberUtil.quiet(()->{super.close();});
        UberUtil.quiet(()->{this.channel.close();});
    }
}
