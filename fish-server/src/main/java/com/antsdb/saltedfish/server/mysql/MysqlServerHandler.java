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

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.KeyStore;
import java.util.Map;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;

import com.antsdb.saltedfish.server.SaltedFish;
import com.antsdb.saltedfish.sql.vdm.ShutdownException;
import com.antsdb.saltedfish.util.CodingError;
import com.antsdb.saltedfish.util.UberUtil;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslHandler;

public class MysqlServerHandler extends ChannelInboundHandlerAdapter implements MysqlConstant {
    static Logger _log = UberUtil.getThisLogger();
    
    SaltedFish fish;
    PacketEncoder packetEncoder;
    SocketChannel channel;
    ChannelWriterNetty out;
    MysqlSession mysession;

    public MysqlServerHandler(SaltedFish fish, SocketChannel ch) {
        this.fish = fish;
        this.channel=ch;
        this.out = new ChannelWriterNetty(ch);
        this.mysession = new MysqlSession(fish, this.out, ch.remoteAddress(), true);
        this.packetEncoder = this.mysession.encoder;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        // process the request with error handling
        
        try {
            run(ctx, (ByteBuf)msg);
        }
        catch (ShutdownException x) {
            shutdown();
        }
        catch (Exception x) {
            if (_log.isDebugEnabled()) {
                String s = msg.toString();
                s = StringUtils.left(s, 8192);
                _log.debug("error detail: \n{}", s, x);
            }
        }
    }
    
    private void run(ChannelHandlerContext ctx, ByteBuf buf) throws Exception {
        int readerIndex = buf.readerIndex() - 4; 
        buf.readerIndex(readerIndex);
        int size = buf.getIntLE(readerIndex) & 0xffffff;
        ByteBuffer niobuf = buf.nioBuffer(readerIndex, size + 4);
        niobuf.order(ByteOrder.LITTLE_ENDIAN);
        buf.readerIndex(readerIndex + size + 4);
        int result = this.mysession.run(niobuf);
        if (result == 1) {
            return;
        }
        else if (result == -1) {
            switchToSSL();
            return;
        }
        else if (result == -2) {
            ctx.close();
            return;
        }
        else {
            throw new IllegalArgumentException();
        }
    }

    private void shutdown() {
        this.fish.close();
        System.exit(0);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        if (this.fish.isClosed()) {
            ctx.close();
            return;
        }
        this.mysession.onConnect();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        this.mysession.close();
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Close the connection when an exception is raised.
        _log.debug("exceptionCaught", cause);
        ctx.close();
    }

    public Map<Integer, MysqlPreparedStatement> getPrepared() {
        return this.mysession.getPrepared();
    }

    public SaltedFish getFish() {
        return this.fish;
    }

    private boolean enableSSL() {
            String keyFile = getFish().getConfig().getSSLKeyFile();
            String password = getFish().getConfig().getSSLPassword();
            if (keyFile!=null || password!=null) {
                return true;
            }
            return false;
    }
    
    public void switchToSSL() {
        if (enableSSL()) {
            String keyFile = getFish().getConfig().getSSLKeyFile();
            String password = getFish().getConfig().getSSLPassword();
            try (FileInputStream keyIn = new FileInputStream(keyFile)) {
                SSLContext serverContext;
                KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());

                byte[] sslKeyVal = IOUtils.toByteArray(keyIn);

                char[] pass = password.toCharArray();
                ks.load(new ByteArrayInputStream(sslKeyVal), pass);

                // Set up key manager factory to use our key store
                KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                kmf.init(ks, pass);

                // Initialize the SSLContext to work with our key managers.
                serverContext = SSLContext.getInstance("TLS");
                serverContext.init(kmf.getKeyManagers(), null, null);
                SSLEngine sslEngine = serverContext.createSSLEngine();
                sslEngine.setUseClientMode(false);
                channel.pipeline().addFirst("ssl", new SslHandler(sslEngine));
            }
            catch (Exception e) {
                throw new CodingError("Failed to switch to SSL: " + e.getMessage());
            }
        }
        else {
            throw new CodingError("ssl.key_file or ssl.password is not set in configuration and ssl is disabled.");
        }
    }
}
