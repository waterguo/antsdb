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
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;

import com.antsdb.saltedfish.beluga.QuorumNode;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class SimpleSocketServer extends TcpServer implements Runnable {
    static Logger _log = UberUtil.getThisLogger();

    private ServerSocketChannel serverChannel;
    private SaltedFish fish;
    private boolean isAux;
    private ExecutorService pool = Executors.newCachedThreadPool(new FishWorkerFactory());
    
    public SimpleSocketServer(SaltedFish fish, boolean isAux) {
        this.fish = fish;
        this.isAux = isAux;
    }
    
    @Override
    public void start(int port) throws IOException {
        this.serverChannel = ServerSocketChannel.open();
        _log.info("starting simple socket listener on port: {}", port);
        this.serverChannel.bind(new InetSocketAddress((InetAddress)null, port));
        Thread thread = new Thread(this);
        thread.setName("fish listener");
        thread.setDaemon(true);
        thread.start();
    }

    @Override
    public void run() {
        try {
            for (;;) {
                SocketChannel channel = this.serverChannel.accept();
                channel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
                Orca orca = this.fish.getOrca();
                if (orca == null) {
                    channel.close();
                    continue;
                }
                if (orca.isClosed()) {
                    channel.close();
                    continue;
                }
                if (!this.isAux && orca.isSlave()) {
                    channel.close();
                    continue;
                }
                if (this.isAux && orca.isSlave()) {
                    if (!isRemoteAllowed(channel.getRemoteAddress())) {
                        channel.close();
                        continue;
                    }
                }
                this.pool.execute(new SimpleSocketWorker(this.fish, channel, this.isAux));
            }
        }
        catch (AsynchronousCloseException ignored) {}
        catch (Exception x) {
            _log.warn("", x);
        }
    }

    private boolean isRemoteAllowed(SocketAddress remote) {
        try {
            InetSocketAddress remotetcp = (InetSocketAddress)remote;
            InetAddress remoteIp = remotetcp.getAddress(); 
            if (remoteIp.isAnyLocalAddress()) {
                return true;
            }
            if (remoteIp.isLoopbackAddress()) {
                return true;
            }
            Map<Long,QuorumNode> nodes = this.fish.orca.getBelugaPod().getQuorum().getNodes();
            for (QuorumNode i:nodes.values()) {
                String nodeDomainName = StringUtils.substringBefore(i.endpoint, ":");
                try {
                    InetAddress nodeIp = InetAddress.getByName(nodeDomainName);
                    if (nodeIp.equals(remoteIp)) {
                        return true;
                    }
                }
                catch (Exception x) {
                    continue;
                }
            }
            return false;
        }
        catch (Exception x) {
            return false;
        }
    }

    @Override
    public void shutdown() {
        try {
            this.serverChannel.close();
        }
        catch (IOException ignored) {}
        this.pool.shutdown();
    }

}
