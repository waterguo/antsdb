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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

import java.sql.SQLException;

import org.slf4j.Logger;

import com.antsdb.saltedfish.server.mysql.packet.StmtClosePacket;
import com.antsdb.saltedfish.server.mysql.packet.StmtExecutePacket;
import com.antsdb.saltedfish.server.mysql.packet.StmtPreparePacket;
import com.antsdb.saltedfish.server.mysql.util.MysqlErrorCode;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.PreparedStatement;
import com.antsdb.saltedfish.sql.vdm.FieldMeta;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * This class handle request, execute and response of prepared statement
 * @author roger
 */
public class PreparedStmtHandler {

    static Logger _log = UberUtil.getThisLogger();

    private MysqlServerHandler serverHandler;
    
    public PreparedStmtHandler(MysqlServerHandler severHandler) {
        this.serverHandler = severHandler;
    }

    /**
     * Handle cmd_stmt_prepare, create a PreparedStaement instance
     * @param ctx
     * @param packet
     * @throws SQLException 
     */
    public void prepare(ChannelHandlerContext ctx,StmtPreparePacket packet) throws SQLException {
    	PreparedStatement script = buildPstmt(packet.sql);
        MysqlPreparedStatement pstmt = new MysqlPreparedStatement(this.serverHandler.fish.getOrca(), script);
        this.serverHandler.getPrepared().put(pstmt.getId(), pstmt);
        responsePrepare(ctx, pstmt);
    }

    public void execute(ChannelHandlerContext ctx, StmtExecutePacket packet) {
        long pstmtId = packet.statementId;
        MysqlPreparedStatement pstmt = this.serverHandler.getPrepared().get((int)pstmtId);
        if (pstmt == null) {
            serverHandler.writeErrMessage(
            		ctx, 
            		MysqlErrorCode.ER_ERROR_WHEN_EXECUTING_COMMAND, 
            		"Unknown pstmtId when executing.");
            return;
        } 
        try {
	        // Using column definition to parse detail info in packet
	        // packet.readFull(pstmt);
	        // packet.values hold BindValue, should we use it or conver it to Parameter?
	        Object result = pstmt.run(this.serverHandler.session);
	        
	    	Helper.writeResonpse(ctx, serverHandler, result, false);
        }
        finally {
        	pstmt.clear();
        }
    }
    
    /**
     * Build PreparedStatemet with sql
     * @param sql
     * @return
     * @throws SQLException
     */
    public PreparedStatement buildPstmt(String sql) throws SQLException{
        // TODO parse sql and get col and param meta, should Script be used?s
        //      how to map it to pstmt;
    	PreparedStatement script = serverHandler.session.prepare(sql);
        return script;
    }

    public void responsePrepare(ChannelHandlerContext ctx, MysqlPreparedStatement pstmt) {
        byte packetId = 0;

        ByteBuf bufferArray = ctx.alloc().buffer();
        PreparedStatement script = pstmt.script;
        int columnCount = (script.getCursorMeta() != null) ? script.getCursorMeta().getColumnCount() : 0;
        // write preparedOk packet
        PacketEncoder.writePacket(
                bufferArray, 
                ++packetId, 
                () -> serverHandler.packetEncoder.writePreparedOKBody(
                        bufferArray, 
                        pstmt.getId(),
                        columnCount,
                        pstmt.getParameterCount()));

        // write parameter field packet
        int parametersNumber = pstmt.getParameterCount();
        if (parametersNumber > 0) {
            for (int i=0; i<parametersNumber; i++) {
                FieldMeta meta = new FieldMeta("", DataType.integer());
                PacketEncoder.writePacket(
                        bufferArray, 
                        ++packetId, 
                        () -> serverHandler.packetEncoder.writeColumnDefBody(bufferArray, meta));
            }
            PacketEncoder.writePacket(
                    bufferArray, 
                    ++packetId, 
                    () -> serverHandler.packetEncoder.writeEOFBody(bufferArray, serverHandler.getSession()));
        }

        // write column field packet
        if (columnCount > 0) {
            for (FieldMeta meta: script.getCursorMeta().getColumns()) {
                PacketEncoder.writePacket(
                        bufferArray, 
                        ++packetId, 
                        () -> serverHandler.packetEncoder.writeColumnDefBody(bufferArray, meta));
            }
            PacketEncoder.writePacket(
                    bufferArray, 
                    ++packetId, 
                    () -> serverHandler.packetEncoder.writeEOFBody(bufferArray, serverHandler.getSession()));
        }

        // send buffer
        ctx.writeAndFlush(bufferArray);
    }
    
    public void close(MysqlServerHandler handler, StmtClosePacket packet) {
    	int stmtId = (int)packet.statementId;
    	MysqlPreparedStatement pstmt = handler.getPrepared().get(stmtId);
    	if (pstmt == null) {
    		return;
    	}
    	pstmt.close();
    	handler.getPrepared().remove(packet.statementId);
    }

}