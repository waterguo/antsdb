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

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.SQLException;
import java.util.BitSet;

import org.slf4j.Logger;

import com.antsdb.mysql.network.PacketPrepare;
import com.antsdb.saltedfish.charset.Decoder;
import com.antsdb.saltedfish.cpp.FlexibleHeap;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.nosql.HumpbackSession;
import com.antsdb.saltedfish.nosql.VaporizingRow;
import com.antsdb.saltedfish.server.mysql.packet.StmtClosePacket;
import com.antsdb.saltedfish.server.mysql.packet.StmtPreparePacket;
import com.antsdb.saltedfish.server.mysql.util.BindValueUtil;
import com.antsdb.saltedfish.server.mysql.util.BufferUtils;
import com.antsdb.saltedfish.server.mysql.util.Fields;
import com.antsdb.saltedfish.server.mysql.util.MysqlErrorCode;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.PreparedStatement;
import com.antsdb.saltedfish.sql.SqlLogger;
import com.antsdb.saltedfish.sql.vdm.Cursor;
import com.antsdb.saltedfish.sql.vdm.FieldMeta;
import com.antsdb.saltedfish.util.AntiCrashCrimeScene;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * This class handle request, execute and response of prepared statement
 * @author xgu0, roger
 */
public class PreparedStmtHandler {

    static Logger _log = UberUtil.getThisLogger();
    
    private MysqlSession mysession;

    public PreparedStmtHandler(MysqlSession mysession) {
        this.mysession = mysession;
    }

    /**
     * Handle cmd_stmt_prepare, create a PreparedStaement instance
     * @param ctx
     * @param packet
     * @throws SQLException 
     */
    public void prepare(StmtPreparePacket packet) throws SQLException {
        PreparedStatement script = buildPstmt(packet.sql);
        MysqlPreparedStatement pstmt = new MysqlPreparedStatement(script);
        this.mysession.getPrepared().put(pstmt.getId(), pstmt);
        responsePrepare(pstmt);
    }

    public void prepare(PacketPrepare packet, Decoder decoder) throws SQLException {
        PreparedStatement script = buildPstmt(packet.getQueryAsCharBuf(decoder));
        MysqlPreparedStatement pstmt = new MysqlPreparedStatement(script);
        this.mysession.getPrepared().put(pstmt.getId(), pstmt);
        responsePrepare(pstmt);
    }

    @SuppressWarnings("unused")
    private void setParameters(Heap heap, MysqlPreparedStatement pstmt, ByteBuffer in, VaporizingRow row) {
        byte flags = BufferUtils.readByte(in);
        long iterationCount = BufferUtils.readUB4(in);
        if (pstmt.getParameterCount() == 0) {
            return;
        }
        
        // read null bitmap
        int nullCount = (pstmt.getParameterCount() + 7) / 8;
        byte[] bytes = new byte[nullCount];
        in.get(bytes);
        BitSet nullBits = BitSet.valueOf(bytes);

        // type information
        int sentTypes = in.get();
        if (sentTypes != 0) {
            byte[] types = new byte[pstmt.getParameterCount()];
            for (int i = 0; i < pstmt.getParameterCount(); i++) {
                types[i] = (byte)BufferUtils.readInt(in);
            }
            pstmt.types = types;
        }

        // bind values
        if (pstmt.types == null) {
            // type information is supposed to be sent in the first execution.
        }
        for (int i = 0; i < pstmt.getParameterCount(); i++) {
            if (nullBits.get(i)) {
                continue;
            }
            if (pstmt.types[i] == Fields.FIELD_TYPE_BLOB) {
                // BLOB values should be set with long data packet
                // and already in pstmt. use isSet to indicate using
                // long data packet in PreparedStmtHandler in execution
                continue;
            }
            long pValue = BindValueUtil.read(heap, in, pstmt.types[i]);
            row.setFieldAddress(i, pValue);
        }
    }

    public String getSql(int statementId) {
        MysqlPreparedStatement pstmt = this.mysession.getPrepared().get(statementId);
        return (pstmt == null) ? null : pstmt.getSql().toString();
    }
    
    public void execute(ByteBuffer packet) {
        int statementId = (int) BufferUtils.readUB4(packet);
        MysqlPreparedStatement pstmt = this.mysession.getPrepared().get(statementId);
        if (pstmt == null) {
            throw new ErrorMessage(MysqlErrorCode.ER_ERROR_WHEN_EXECUTING_COMMAND, "Unknown prepared statement ID");
        }
        AntiCrashCrimeScene.log(pstmt.script.getSql(), packet);
        try (Heap heap = new FlexibleHeap()) {
            VaporizingRow row = pstmt.createRow(heap);
            packet.mark();
            setParameters(heap, pstmt, packet, row);
            pstmt.preExecute(row);
            packet.reset();
            execute(pstmt, row, pstmt.types, packet);
        }
    }
    
    private void execute(MysqlPreparedStatement pstmt, VaporizingRow row, byte[] types, ByteBuffer input) {
        boolean success = false;
        try {
            // Using column definition to parse detail info in packet
            // packet.readFull(pstmt);
            // packet.values hold BindValue, should we use it or convert it to Parameter?
            HumpbackSession hsession = this.mysession.session.getHSession();
            long startLp = hsession.getLastLp();
            pstmt.run(this.mysession.session, row, (result)-> {
                if (hsession.getLastLp() != startLp) {
                    SqlLogger logger = this.mysession.session.getOrca().getSqlLogger();
                    logger.logWrite(mysession.session, "execute", result, pstmt.script.getSql(), types, input);
                }
                writeResult(pstmt, result);
            });
            success = true;
        }
        finally {
            if (!success) {
                _log.debug("sql: {}", pstmt.script.getSql().toString());
                _log.debug("statement parameters:\n{}", row);
            }
            pstmt.clear();
        }
    }
    
    private void writeResult(MysqlPreparedStatement pstmt, Object result) {
        if (result instanceof Cursor) {
            Cursor c = (Cursor)result;
            try {
                if (pstmt.meta == null) {
                    ChannelWriterMemory buf = null;
                    try {
                        buf = new ChannelWriterMemory();
                        Helper.writeCursorMeta(buf,
                                               this.mysession.session, 
                                               this.mysession.encoder, 
                                               (Cursor)result);
                        pstmt.meta = (ByteBuffer)buf.getWrapped();
                        pstmt.packetSequence = this.mysession.encoder.packetSequence;
                    }
                    finally {
                        if (pstmt.meta == null && buf != null) {
                            buf.close();
                        }
                    }
                }
                pstmt.meta.flip();
                this.mysession.encoder.packetSequence = pstmt.packetSequence;
                Helper.writeCursor(mysession.out, mysession, c, pstmt.meta, false);
            }
            finally {
                c.close();
            }
        }
        else {
            Helper.writeResonpse(mysession.out, mysession, result, false);
        }
    }

    /**
     * Build PreparedStatemet with sql
     * @param sql
     * @return
     * @throws SQLException
     */
    public PreparedStatement buildPstmt(CharBuffer sql) throws SQLException{
        // TODO parse sql and get col and param meta, should Script be used?s
        //      how to map it to pstmt;
        PreparedStatement script = mysession.session.prepare(sql);
        return script;
    }

    public void responsePrepare(MysqlPreparedStatement pstmt) {
        PreparedStatement script = pstmt.script;
        int columnCount = (script.getCursorMeta() != null) ? script.getCursorMeta().getColumnCount() : 0;
        // write preparedOk packet
        this.mysession.encoder.writePacket(
                mysession.out,
                (packet) -> mysession.encoder.writePreparedOKBody(
                        packet, 
                        pstmt.getId(),
                        columnCount,
                        pstmt.getParameterCount()));

        // write parameter field packet
        int parametersNumber = pstmt.getParameterCount();
        if (parametersNumber > 0) {
            for (int i=0; i<parametersNumber; i++) {
                FieldMeta meta = new FieldMeta("", DataType.integer());
                this.mysession.encoder.writePacket(
                        mysession.out, 
                        (packet) -> mysession.encoder.writeColumnDefBody(packet, meta));
            }
            this.mysession.encoder.writePacket(
                    mysession.out, 
                    (packet) -> mysession.encoder.writeEOFBody(packet, mysession.session));
        }

        // write column field packet
        if (columnCount > 0) {
            for (FieldMeta meta: script.getCursorMeta().getColumns()) {
                this.mysession.encoder.writePacket(
                        mysession.out, 
                        (packet) -> mysession.encoder.writeColumnDefBody(packet, meta));
            }
            this.mysession.encoder.writePacket(
                    mysession.out, 
                    (packet) -> mysession.encoder.writeEOFBody(packet, mysession.session));
        }

        // send buffer
        this.mysession.out.flush();
    }
    
    public void close(MysqlServerHandler handler, StmtClosePacket packet) {
        int stmtId = (int) packet.statementId;
        close(stmtId);
    }

    public void close(int statementId) {
        MysqlPreparedStatement pstmt = this.mysession.getPrepared().get(statementId);
        if (pstmt == null) {
            return;
        }
        pstmt.close();
        this.mysession.getPrepared().remove(statementId);
    }
}