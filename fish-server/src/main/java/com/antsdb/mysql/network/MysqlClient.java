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

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.MessageToMessageDecoder;

/**
 * 
 * @author wgu0
 */
public class MysqlClient {
	String host;
	int port;
	ChannelFuture future;
	MysqlClientState state = new MysqlClientState();
	
	class StateMonitor extends MessageToMessageDecoder<ByteBuf> {
		@Override
		protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
			ByteBuf packet = (ByteBuf)msg;
			packet.retain();
			getState().notifyReceive(packet);
			out.add(new MysqlPacket(getState().getPacketType(packet), packet));
		}
	}
	
	public MysqlClient(String host, int port) {
		this.host = host;
		this.port = port;
	}
	
	public void start(EventLoopGroup pool, ChannelHandler handler) throws InterruptedException {
		Bootstrap b = new Bootstrap();
        ChannelInitializer<SocketChannel> initializer = new ChannelInitializer<SocketChannel>() {
			@Override
			protected void initChannel(SocketChannel ch) throws Exception {
				ch.pipeline().addLast(new MysqlPacketDecoder(), new StateMonitor(), handler);
			}
        };
		b.group(pool)
		 .channel(NioSocketChannel.class)
		 .option(ChannelOption.TCP_NODELAY, true)
		 .handler(initializer);
		this.future = b.connect(this.host, this.port).sync();
	}

	public void close() {
		this.future.channel().close();
	}

	public void write(ByteBuf packet) {
		this.state.notifySend(packet);
		this.future.channel().write(packet);
	}

	public void flush() {
		this.future.channel().flush();
	}
	
	public MysqlClientState getState() {
		return this.state;
	}
}
