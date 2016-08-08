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

import org.slf4j.Logger;

import com.antsdb.saltedfish.server.SaltedFish;
import com.antsdb.saltedfish.util.UberUtil;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

public class MysqlChannelInitializer extends ChannelInitializer<SocketChannel> {
    static Logger _log = UberUtil.getThisLogger();
    
    SaltedFish fish;
    
    public MysqlChannelInitializer(SaltedFish fish) {
        super();
        this.fish = fish;
    }

    @Override
    public void initChannel(SocketChannel ch) throws Exception {
        _log.trace("channel initialized with remote address {}", ch.remoteAddress());
        MysqlServerHandler handler = new MysqlServerHandler(fish);
        PacketDecoder decoder = new PacketDecoder(handler);
        ch.pipeline().addLast(decoder, handler);
    }
}
