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
package com.antsdb.saltedfish.server;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;

import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class SimpleSocketServer extends TcpServer implements Runnable {
    static Logger _log = UberUtil.getThisLogger();

    private ServerSocketChannel serverChannel;
    private SaltedFish fish;
    private ExecutorService pool = Executors.newCachedThreadPool();
    
    public SimpleSocketServer(SaltedFish fish) {
        this.fish = fish;
    }
    
    @Override
    public void start(int port) throws IOException {
        this.serverChannel = ServerSocketChannel.open();
        _log.info("starting simple socket listener on port: {}", port);
        this.serverChannel.bind(new InetSocketAddress((InetAddress)null, port));
        Thread thread = new Thread(this);
        thread.setName("fish listener");
        thread.start();
    }

    @Override
    public void run() {
        try {
            for (;;) {
                SocketChannel channel = this.serverChannel.accept();
                this.pool.execute(new SimpleSocketWorker(this.fish, channel));
                // new Thread().start();;
            }
        }
        catch (Exception x) {
            _log.warn("", x);
        }
    }

    @Override
    public void shutdown() throws Exception {
        this.pool.shutdown();
    }

}
