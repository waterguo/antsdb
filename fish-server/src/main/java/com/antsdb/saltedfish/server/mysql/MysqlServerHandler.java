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

import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.codec.Charsets;
import org.slf4j.Logger;

import com.antsdb.saltedfish.charset.Codecs;
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
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.Session;
import com.antsdb.saltedfish.sql.vdm.Use;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.util.CodingError;
import com.antsdb.saltedfish.util.UberUtil;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class MysqlServerHandler extends ChannelInboundHandlerAdapter implements MysqlConstant {
    final static int SERVER_VERSION_LENGTH = 60;
    final static int HEADER_LENGTH = 4;
    static Logger _log = UberUtil.getThisLogger();
//    public static String DEFAULT_CHARSET = "UFT8";
    public static String SERVER_VERSION = Orca.VERSION;
    public static byte PROTOCOL_VERSION = 0x0a;
    public static byte CHAR_SET_INDEX = 0x21;
    public static String AUTH_MYSQL_NATIVE = "mysql_native_password";
    
    public static long threadId= 300;
    
    SaltedFish fish;
    Session session;
    PreparedStmtHandler preparedStmtHandler;
    QueryHandler queryHandler;
    PacketEncoder packetEncoder;
    Map<Integer, MysqlPreparedStatement> prepared = new HashMap<>();
    String charset;
	private Decoder decoder;
	int packetSequence;
    
    public MysqlServerHandler(SaltedFish fish) {
        this.fish = fish;
        packetEncoder = new PacketEncoder();
		this.decoder = Codecs.ISO8859;
		this.packetEncoder.setCodec(Charsets.ISO_8859_1, MYSQL_CHARSET_NAME_binary);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (_log.isTraceEnabled()) {
            _log.trace(msg.toString());
        }
        
        // process the request with error handling
        
        try {
            run(ctx, msg);
        }
        catch (Exception x) {
            if (_log.isDebugEnabled()) {
                _log.debug("error detail: \n{}", msg, x);
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
            auth(ctx, (AuthPacket)msg);
            return;
        }
        else if (msg instanceof ShutdownPacket) {
            ctx.channel().close();
            ctx.channel().parent().close();
            System.exit(0);
        }
        if (this.session == null) {
        	throw new OrcaException("session is closed");
        }
        if (this.session.isClosed()) {
        	throw new OrcaException("session is closed");
        }
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

	@Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
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
        		status));

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
		_log.warn("exceptionCaught", cause);
        ctx.close();
    }

    private void auth(ChannelHandlerContext ctx, AuthPacket request) {
        // logging in
        
        this.session = this.fish.getOrca().createSession(request.user);
        this.session.setCurrentNamespace(request.dbname);
        if (_log.isTraceEnabled()) {
            _log.debug("Connection user:" + request.user);
            _log.debug("Connection default schema:" + request.dbname);
        }
        preparedStmtHandler = new PreparedStmtHandler(this);
        queryHandler = new QueryHandler(this);
        
        // if client send client_secure_connection, send back auth ok to client
        
        if ((request.clientParam & CLIENT_SECURE_CONNECTION)!=0) {
            ByteBuf buf = ctx.alloc().buffer();
            buf.writeBytes(PacketEncoder.AUTH_OK_PACKET);
            ctx.writeAndFlush(buf);
        }
    }

    private void init(ChannelHandlerContext ctx, InitPacket request) {
    	VdmContext vdm = new VdmContext(session, 0);
    	new Use(request.database).run(vdm, null);
        ByteBuf bufferArray = ctx.alloc().buffer();
        bufferArray.writeBytes(PacketEncoder.OK_PACKET);
        ctx.writeAndFlush(bufferArray);
	}

    private void resetCharset() throws Exception {
        if (this.charset != getSession().getProtocolCharset()) {
        	// reset encoder and decoder
        	String name = getSession().getProtocolCharset();
        	if (name.equals("latin1")) {
        		this.decoder = Codecs.ISO8859;
        		this.packetEncoder.setCodec(Charsets.ISO_8859_1, MYSQL_CHARSET_NAME_binary);
        	}
        	else if (name.equals("cp1250")) {
        		this.decoder = Codecs.ISO8859;
        		this.packetEncoder.setCodec(Charsets.ISO_8859_1, MYSQL_CHARSET_NAME_binary);
        	}
        	else if (name.equals("utf8")) {
        		this.decoder = Codecs.UTF8;
        		this.packetEncoder.setCodec(Charsets.UTF_8, MYSQL_CHARSET_NAME_utf8);
        	}
        	else if (name.equals("utf8mb4")) {
        		this.decoder = Codecs.UTF8;
        		this.packetEncoder.setCodec(Charsets.UTF_8, MYSQL_CHARSET_NAME_utf8);
        	}
        	else {
        		throw new Exception("unknown charset: " + name);
        	}
        	this.charset = name;
        }
    }
    
    private void query(ChannelHandlerContext ctx, QueryPacket request) throws Exception {
        queryHandler.query(ctx, request);
        if (this.charset != getSession().getProtocolCharset()) {
        	this.resetCharset();
        }
    }

    private void stmtPrepare(ChannelHandlerContext ctx, StmtPreparePacket pstmtPacket) throws SQLException {
        if (preparedStmtHandler != null) {
            String sql = pstmtPacket.sql;
            if (sql == null || sql.length() == 0) {
                writeErrMessage(ctx,MysqlErrorCode.ER_NOT_ALLOWED_COMMAND, "Empty SQL");
                return;
            }

            preparedStmtHandler.prepare(ctx, pstmtPacket);
        } 
        else {
            writeErrMessage(ctx,MysqlErrorCode.ER_UNKNOWN_COM_ERROR,
                    "Prepare unsupported!");
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
        } else {
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

    public void writeErrMessage(ChannelHandlerContext ctx, int errno, String msg) {
        ByteBuf buf = ctx.alloc().buffer();
        PacketEncoder.writePacket(buf, (byte)1, () -> packetEncoder.writeErrorBody(
        		buf, 
        		errno, 
        		Charset.defaultCharset().encode(msg)));
        ctx.writeAndFlush(buf);
    }

    /**
     * need double check if these are enough for tpcc
     * @return
     */
    protected static int getServerCapabilities() {
        int flag = 0;
        flag |= CLIENT_CONNECT_WITH_DB;
        flag |= CLIENT_PROTOCOL_41;
        flag |= CLIENT_TRANSACTIONS;
        flag |= CLIENT_RESERVED;
        flag |= CLIENT_PLUGIN_AUTH;
        flag |= CLIENT_SECURE_CONNECTION;
        return flag;
    }

    public Map<Integer, MysqlPreparedStatement> getPrepared() {
        return this.prepared;
    }

	public Session getSession() {
		return this.session;
	}

	public Decoder getDecoder() {
		return this.decoder != null ? this.decoder : Codecs.ISO8859;
	}

	public SaltedFish getFish() {
		return this.fish;
	}

	byte getNextPacketSequence() {
		return (byte)++this.packetSequence;
	}
}
