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
package com.antsdb.mysql.network;

import java.util.function.Supplier;

import org.slf4j.Logger;

import com.antsdb.saltedfish.util.UberUtil;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * 
 * @author wgu0
 */
public class MysqlServer {
    static Logger _log = UberUtil.getThisLogger();

    int port;
    ChannelFuture future;
    
    public MysqlServer(int port) {
		this.port = port;
	}
	
	public void start(EventLoopGroup pool, Supplier<ChannelHandler> supplier) throws InterruptedException {
        ServerBootstrap b = new ServerBootstrap();
        ChannelInitializer<SocketChannel> initializer = new ChannelInitializer<SocketChannel>() {
			@Override
			protected void initChannel(SocketChannel ch) throws Exception {
				ch.pipeline().addLast(new MysqlPacketDecoder(),  supplier.get());
			}
        };
        b.group(pool, pool)
         .channel(NioServerSocketChannel.class)
         .childHandler(initializer)
         .option(ChannelOption.SO_BACKLOG, 128)
         .childOption(ChannelOption.SO_KEEPALIVE, true);

        // Bind and start to accept incoming connections.

        _log.info("starting netty on port: " + this.port);
        this.future = b.bind(this.port).sync();
	}
}
