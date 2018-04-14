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
package com.antsdb.saltedfish.server;

import org.slf4j.Logger;

import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.util.UberUtil;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * 
 * @author *-xguo0<@
 */
class SaltedFishWeb {
    static final Logger _log = UberUtil.getThisLogger();
    
    Orca orca;
    EventLoopGroup bossGroup;
    NioEventLoopGroup workerGroup;
    ChannelFuture f;

    private int port;

    SaltedFishWeb(Orca orca) {
        this.orca = orca;
    }

    void start() {
        bossGroup = new NioEventLoopGroup();
        workerGroup = new NioEventLoopGroup(1);

        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
         .channel(NioServerSocketChannel.class)
         .childHandler(new HttpInitializer(this.orca))
         .option(ChannelOption.SO_BACKLOG, 128)
         .childOption(ChannelOption.SO_KEEPALIVE, true);

        // Bind and start to accept incoming connections.

        for (int i=0; i<10; i++) {
            int port = i + 2011;
            try {
                this.f = b.bind(port).sync();
                this.port = port;
                break;
            }
            catch (Exception ignored) {}
        }
        if (port == 0) {
            _log.warn("unable to bind web port");
        }
        else {
            _log.info("starting web on port: " + this.port);
        }
    }
}
