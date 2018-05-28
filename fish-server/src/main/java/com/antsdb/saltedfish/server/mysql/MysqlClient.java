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

import org.slf4j.Logger;

import com.antsdb.saltedfish.server.SaltedFish;
import com.antsdb.saltedfish.util.CodingError;
import com.antsdb.saltedfish.util.UberUtil;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

/**
 * 
 * @author rluo
 */
public class MysqlClient {
	static Logger _log = UberUtil.getThisLogger();
	
    private static MysqlClient client = null;
    
    String host;
	int port;
	ChannelFuture future;
	
	// status of replication client (slave)
	static Boolean isRunning = false;
	
	private MysqlClient() {
		this.host = SaltedFish.getInstance().getConfig().getProperties().getProperty("masterHost");
		this.port = Integer.parseInt(SaltedFish.getInstance().getConfig().getProperties().getProperty("masterPort"));
	}
	
	// start slave, sync with stop to prevent multi replication
    public static void start()
    {
        if (client == null) {
            synchronized(MysqlClient.class) {
                if (client == null)
                {
                    client = new MysqlClient(); 
                }
            }
        }
        if (client != null)
        {
        	synchronized(isRunning)
        	{
        		if (!isRunning)
        		{
			        NioEventLoopGroup pool = new NioEventLoopGroup();
			        try
			        {
			        	client.run(pool, new MysqlClientHandler());
			        	isRunning = true;
			        }
			        catch (Exception e)
			        {
			        	_log.error("", e);
			        	throw new CodingError("Failed to start replication");
			        }
        		}
        	}
        }
    	
    }

    // stop slave
    public static void stop()
    {
        if (client != null) {
        	synchronized(isRunning)
        	{
        		if (isRunning)
        		{
        			client.close();
        			isRunning = false;
        		}
        	}
        }
    }

    private void run(EventLoopGroup pool, MysqlClientHandler handler) throws InterruptedException {
		Bootstrap b = new Bootstrap();
        ChannelInitializer<SocketChannel> initializer = new ChannelInitializer<SocketChannel>() {
			@Override
			protected void initChannel(SocketChannel ch) throws Exception {
				ch.pipeline().addLast(new ReplicationPacketDecoder(handler), handler);
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
		this.future.channel().write(packet);
	}

	public void flush() {
		this.future.channel().flush();
	}
	
	
}

