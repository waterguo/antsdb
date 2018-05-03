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

import com.antsdb.saltedfish.server.mysql.MysqlChannelInitializer;
import com.antsdb.saltedfish.util.UberUtil;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * 
 * @author *-xguo0<@
 */
public class NettyServer extends TcpServer {
    static Logger _log = UberUtil.getThisLogger();

    private int bossGroupSize;
    private int workerGroupSize;
    private SaltedFish fish;

    private NioEventLoopGroup bossGroup;

    private NioEventLoopGroup workerGroup;

    NettyServer(SaltedFish fish, int bossGroupSize, int workerGroupSize) {
        this.fish = fish;
        this.bossGroupSize = bossGroupSize;
        this.workerGroupSize = workerGroupSize;
    }
    
    @Override
    public void start(int port) throws Exception {
        this.bossGroup = new NioEventLoopGroup(this.bossGroupSize);
        this.workerGroup = new NioEventLoopGroup(this.workerGroupSize);
        _log.info("netty worker pool size: {}", workerGroup.executorCount());

        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
         .channel(NioServerSocketChannel.class)
         .childHandler(new MysqlChannelInitializer(this.fish))
         .option(ChannelOption.SO_BACKLOG, 128)
         .childOption(ChannelOption.SO_SNDBUF, 16 * 1024)
         .childOption(ChannelOption.SO_KEEPALIVE, true);

        // Bind and start to accept incoming connections.

        _log.info("starting netty listener on port: {}", port);
        b.bind(port).sync();
    }

    @Override
    public void shutdown() throws Exception {
        // Wait until the server socket is closed.
        // In this example, this does not happen, but you can do that to gracefully
        // shut down your server.
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
    }

}
