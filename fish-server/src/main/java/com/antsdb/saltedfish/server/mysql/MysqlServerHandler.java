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

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.security.KeyStore;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;

import com.antsdb.saltedfish.charset.Decoder;
import com.antsdb.saltedfish.server.SaltedFish;
import com.antsdb.saltedfish.server.mysql.packet.AuthPacket;
import com.antsdb.saltedfish.server.mysql.packet.ClosePacket;
import com.antsdb.saltedfish.server.mysql.packet.FieldListPacket;
import com.antsdb.saltedfish.server.mysql.packet.InitPacket;
import com.antsdb.saltedfish.server.mysql.packet.LongDataPacket;
import com.antsdb.saltedfish.server.mysql.packet.PingPacket;
import com.antsdb.saltedfish.server.mysql.packet.QueryPacket;
import com.antsdb.saltedfish.server.mysql.packet.SetOptionPacket;
import com.antsdb.saltedfish.server.mysql.packet.ShutdownPacket;
import com.antsdb.saltedfish.server.mysql.packet.StmtClosePacket;
import com.antsdb.saltedfish.server.mysql.packet.StmtExecutePacket;
import com.antsdb.saltedfish.server.mysql.packet.StmtPreparePacket;
import com.antsdb.saltedfish.server.mysql.util.MysqlErrorCode;
import com.antsdb.saltedfish.sql.AuthPlugin;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.Session;
import com.antsdb.saltedfish.sql.vdm.Use;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.storage.KerberosHelper;
import com.antsdb.saltedfish.util.CodingError;
import com.antsdb.saltedfish.util.UberUtil;
import static com.antsdb.saltedfish.server.mysql.util.MysqlErrorCode.*;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslHandler;

public class MysqlServerHandler extends ChannelInboundHandlerAdapter implements MysqlConstant {
    final static int SERVER_VERSION_LENGTH = 60;
    final static int HEADER_LENGTH = 4;
    static Logger _log = UberUtil.getThisLogger();
    //    public static String DEFAULT_CHARSET = "UFT8";
    public static String SERVER_VERSION = Orca._version;
    public static byte PROTOCOL_VERSION = 0x0a;
    public static byte CHAR_SET_INDEX = 0x21;
    public static String AUTH_MYSQL_CLEARTEXT = "mysql_clear_password";
    public static String AUTH_MYSQL_NATIVE = "mysql_native_password";
    
    public static long threadId= 300;
    byte[] seed  = new byte[] {0x50, 0x3a, 0x6e, 0x3d, 0x25, 0x40, 0x51, 0x56, 0x73, 0x68, 0x2f, 0x50, 
            0x27, 0x6f, 0x7a, 0x38, 0x46, 0x38, 0x26, 0x51};
    
    SaltedFish fish;
    Session session;
    PreparedStmtHandler preparedStmtHandler;
    QueryHandler queryHandler;
    PacketEncoder packetEncoder;
    Map<Integer, MysqlPreparedStatement> prepared = new HashMap<>();
    int packetSequence;
    
    public SocketChannel chanel;
    
    public boolean isHandshaken = false;
    public boolean sslConnected = false;
    private AuthPlugin authPlugin;

    public MysqlServerHandler(SaltedFish fish, SocketChannel ch) {
        this.fish = fish;
        this.chanel=ch;
        packetEncoder = new PacketEncoder(this);
        this.authPlugin = this.fish.getOrca().getAuthPlugin();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if ((this.session != null) && this.session.isClosed()) {
            return;
        }
        
        if (_log.isTraceEnabled()) {
            _log.trace(msg.toString());
        }
        
        // process the request with error handling
        
        try {
            run(ctx, msg);
        }
        catch (Exception x) {
            if (_log.isDebugEnabled()) {
                String s = msg.toString();
                s = StringUtils.left(s, 8192);
                _log.debug("error detail: \n{}", s, x);
            }
            String error = x.getMessage()==null? x.toString() : x.getMessage();
            writeErrMessage(ctx, MysqlErrorCode.ER_ERROR_WHEN_EXECUTING_COMMAND, error);
        }
        
        // flush and finish
        
        ctx.flush();
        if (_log.isTraceEnabled()) {
            _log.trace("end");
        }
    }

    
    private void run(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof AuthPacket) {
            AuthPacket authmsg = (AuthPacket)msg;
            // if it's SSL packet, switch to ssl connection
            if (authmsg.isSSL)
                switchToSSL();
            else
                auth(ctx, (AuthPacket)msg);
            return;
        }
        else if (msg instanceof ShutdownPacket) {
            ctx.channel().close();
            ctx.channel().parent().close();
            shutdown();
        }
        if (this.session == null) {
            throw new OrcaException("session is closed");
        }
        if (this.session.isClosed()) {
            throw new OrcaException("session is closed");
        }
        try {
            this.session.notifyStartQuery();
            if (msg instanceof PingPacket) {
                ping(ctx);
            }
            else if (msg instanceof QueryPacket) {
                query(ctx, (QueryPacket)msg);
            }
            else if (msg instanceof InitPacket) {
                init(ctx, (InitPacket)msg);
            }
            else if (msg instanceof StmtPreparePacket) {
                stmtPrepare(ctx, (StmtPreparePacket)msg);
            }
            else if (msg instanceof LongDataPacket) {
                stmtPrepareLongData(ctx, (LongDataPacket)msg);
            }
            else if (msg instanceof StmtExecutePacket) {
                stmtExecute(ctx, (StmtExecutePacket)msg);
            }
            else if (msg instanceof StmtClosePacket) {
                stmtClose(ctx, (StmtClosePacket)msg);
            }
            else if (msg instanceof ClosePacket) {
                close(ctx);
            }
            else if (msg instanceof FieldListPacket) {
                fieldList(ctx, (FieldListPacket)msg);
            }
            else if (msg instanceof SetOptionPacket) {
                setOption(ctx, (SetOptionPacket)msg);
            }
            else {
                unknown(ctx);
                throw new CodingError("unknown message type: " + msg.getClass().toString());
            }
        }
        finally {
            this.session.notifyEndQuery();
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
        
        int packLength = 40;
        ByteBuf buf = ctx.alloc().buffer(packLength);
        
        threadId ++;
        // status is set to SERVER_STATUS_AUTOCOMMIT 0x0002
        int status = 0x0002;
        PacketEncoder.writePacket(buf, (byte)0, () -> PacketEncoder.writeHandshakeBody(
                buf, 
                SERVER_VERSION,  
                PROTOCOL_VERSION, 
                threadId, 
                getServerCapabilities(), 
                CHAR_SET_INDEX, 
                status,
                this.authPlugin));
        ctx.writeAndFlush(buf);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            if (this.session != null) {
                for (MysqlPreparedStatement i:this.prepared.values()) {
                    i.close();
                }
                this.fish.getOrca().closeSession(this.session);
            }
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Close the connection when an exception is raised.
        _log.trace("exceptionCaught", cause);
        ctx.close();
    }

    private void auth(ChannelHandlerContext ctx, AuthPacket request) {
        if (!this.authPlugin.authenticate(request.user, request.passwordRaw)) {
            writeErrMessage(ctx, ER_ACCESS_DENIED_ERROR,"Access denied for invalid user or password");
        }
        
        // logging in
        
        this.session = this.fish.getOrca().createSession(request.user, this.chanel.remoteAddress().toString());
        this.session.setCurrentNamespace(request.dbname);
        if (_log.isTraceEnabled()) {
            _log.debug("Connection user:" + request.user);
            _log.debug("Connection default schema:" + request.dbname);
        }
        preparedStmtHandler = new PreparedStmtHandler(this);
        queryHandler = new QueryHandler(this);
        
        // Enable kerberos by default, set "kerberos.auth_disable" in conf.properties to disable it
        if (!authEnable() || !kerberosEnable() || KerberosHelper.login(request.user, request.password)) {
            if (authEnable() && !kerberosEnable() && !testPassword(request.password)) {
                    writeErrMessage(ctx, ER_ACCESS_DENIED_ERROR, "Cleartext test failed");
            } 
            else {
                boolean result = this.authPlugin.authenticate(request.user, request.passwordRaw);
                if (result) {
                        // send back auth ok to client
                        ByteBuf buf = ctx.alloc().buffer();
                        buf.writeBytes(this.sslConnected? PacketEncoder.SSL_AUTH_OK_PACKET : PacketEncoder.AUTH_OK_PACKET);
                        ctx.writeAndFlush(buf);
                } 
                else {
                    writeErrMessage(ctx, ER_ACCESS_DENIED_ERROR, "Access denied for invalid user or password");
                }
            }
        } 
        else {
            writeErrMessage(ctx, ER_ACCESS_DENIED_ERROR, "Access denied for invalid user or password");
        }
    }

    private boolean testPassword(String pwd) {
            String testPwd = getFish().getConfig().getTestPassword();
            return testPwd==null?  true: testPwd.equals(pwd);
    }

    public boolean authEnable() {
            return getFish().getConfig().getAuthEnable().equalsIgnoreCase("Y");
    }

    public boolean kerberosEnable() {
            return getFish().getConfig().getKerberosEnable().equalsIgnoreCase("Y");
    }

    private void init(ChannelHandlerContext ctx, InitPacket request) {
            VdmContext vdm = new VdmContext(session, 0);
            new Use(request.database).run(vdm, null);
        ByteBuf bufferArray = ctx.alloc().buffer();
        bufferArray.writeBytes(PacketEncoder.OK_PACKET);
        ctx.writeAndFlush(bufferArray);
    }
    
    private void query(ChannelHandlerContext ctx, QueryPacket request) throws Exception {
        queryHandler.query(ctx, request);
    }

    private void stmtPrepare(ChannelHandlerContext ctx, StmtPreparePacket pstmtPacket) throws SQLException {
        if (preparedStmtHandler != null) {
            CharBuffer sql = pstmtPacket.sql;
            if (sql == null || sql.length() == 0) {
                writeErrMessage(ctx,MysqlErrorCode.ER_NOT_ALLOWED_COMMAND, "Empty SQL");
                return;
            }
            preparedStmtHandler.prepare(ctx, pstmtPacket);
        } 
        else {
            writeErrMessage(ctx,MysqlErrorCode.ER_UNKNOWN_COM_ERROR, "Prepare unsupported!");
        }
    }

    private void stmtPrepareLongData(ChannelHandlerContext ctx, LongDataPacket dataPacket) throws SQLException {
        if (preparedStmtHandler != null) {
            dataPacket.read_(this);
        } 
        else {
            writeErrMessage(ctx,MysqlErrorCode.ER_UNKNOWN_COM_ERROR,
                    "Prepare unsupported!");
        }
    }

    private void stmtExecute(ChannelHandlerContext ctx,StmtExecutePacket pstmtPacket) {
        if (preparedStmtHandler != null) {
            pstmtPacket.read_(this);
            preparedStmtHandler.execute(ctx, pstmtPacket);
        } 
        else {
            writeErrMessage(ctx, MysqlErrorCode.ER_UNKNOWN_COM_ERROR,
                    "Prepare unsupported!");
        }
    }

    private void fieldList(ChannelHandlerContext ctx,FieldListPacket pstmtPacket) {
        writeErrMessage(ctx, MysqlErrorCode.ER_UNKNOWN_COM_ERROR,
                "Field list unsupported!");
    }

    private void setOption(ChannelHandlerContext ctx,SetOptionPacket pstmtPacket) {
        writeErrMessage(ctx, MysqlErrorCode.ER_UNKNOWN_COM_ERROR,
                "Set option unsupported!");
    }

    private void stmtClose(ChannelHandlerContext ctx, StmtClosePacket pstmtPacket) {
        if (preparedStmtHandler != null) {
            preparedStmtHandler.close(this, pstmtPacket);
        } 
        else {
            writeErrMessage(ctx, MysqlErrorCode.ER_UNKNOWN_COM_ERROR,
                    "Prepare unsupported!");
        }
    }

    private void close(ChannelHandlerContext ctx) {
            ctx.close();
    }

    private void ping(ChannelHandlerContext ctx) {
        ByteBuf bufferArray = ctx.alloc().buffer();
        bufferArray.writeBytes(PacketEncoder.OK_PACKET);
        ctx.writeAndFlush(bufferArray);
    }

    private void unknown(ChannelHandlerContext ctx) {
        writeErrMessage(ctx,MysqlErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
    }

    public ChannelFuture writeErrMessage(ChannelHandlerContext ctx, int errno, String msg) {
        ByteBuf buf = ctx.alloc().buffer();
        PacketEncoder.writePacket(buf, (byte)1, () -> packetEncoder.writeErrorBody(
                buf, 
                errno, 
                Charset.defaultCharset().encode(msg)));
        return ctx.writeAndFlush(buf);
    }

    /**
     * need double check if these are enough for tpcc
     * @return
     */
    protected int getServerCapabilities() {
        int flag = 0;
        flag |= CLIENT_CONNECT_WITH_DB;
        flag |= CLIENT_PROTOCOL_41;
        flag |= CLIENT_TRANSACTIONS;
        flag |= CLIENT_RESERVED;
        flag |= CLIENT_PLUGIN_AUTH;
        flag |= CLIENT_SECURE_CONNECTION;
        if (enableSSL()) {
                flag |= CLIENT_SSL;
        }
        return flag;
    }

    public Map<Integer, MysqlPreparedStatement> getPrepared() {
        return this.prepared;
    }

    public Session getSession() {
        return this.session;
    }

    public Decoder getDecoder() {
        Decoder result = null;
        result = this.session.getConfig().getRequestDecoder();
        return result;
    }

    public SaltedFish getFish() {
        return this.fish;
    }

    byte getNextPacketSequence() {
        return (byte)++this.packetSequence;
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
                KeyStore ks = KeyStore.getInstance("JKS");

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
                chanel.pipeline().addFirst("ssl", new SslHandler(sslEngine));

                // indicate SSL is established
                sslConnected = true;
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
