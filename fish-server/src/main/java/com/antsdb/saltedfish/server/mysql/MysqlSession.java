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

import static com.antsdb.saltedfish.server.mysql.MysqlConstant.*;
import static com.antsdb.saltedfish.server.mysql.util.MysqlErrorCode.ERR_FOUND_EXCEPION;
import static com.antsdb.saltedfish.server.mysql.util.MysqlErrorCode.ER_ACCESS_DENIED_ERROR;
import static com.antsdb.saltedfish.server.mysql.util.MysqlErrorCode.ER_ERROR_WHEN_EXECUTING_COMMAND;
import static com.antsdb.saltedfish.server.mysql.util.MysqlErrorCode.ER_UNKNOWN_COM_ERROR;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.antlr.v4.runtime.InputMismatchException;
import org.antlr.v4.runtime.NoViableAltException;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.antsdb.mysql.network.Packet;
import com.antsdb.mysql.network.PacketDbInit;
import com.antsdb.mysql.network.PacketExecute;
import com.antsdb.mysql.network.PacketFieldList;
import com.antsdb.mysql.network.PacketFishRestoreEnd;
import com.antsdb.mysql.network.PacketFishRestoreStart;
import com.antsdb.mysql.network.PacketLogReplicate;
import com.antsdb.mysql.network.PacketLongData;
import com.antsdb.mysql.network.PacketPing;
import com.antsdb.mysql.network.PacketPrepare;
import com.antsdb.mysql.network.PacketQuery;
import com.antsdb.mysql.network.PacketQuit;
import com.antsdb.mysql.network.PacketSetOption;
import com.antsdb.mysql.network.PacketStmtClose;
import com.antsdb.saltedfish.charset.Decoder;
import com.antsdb.saltedfish.cpp.AllocPoint;
import com.antsdb.saltedfish.cpp.MemoryManager;
import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.RowIterator;
import com.antsdb.saltedfish.nosql.TrxMan;
import com.antsdb.saltedfish.server.SaltedFish;
import com.antsdb.saltedfish.server.fishnet.PacketFishBackup;
import com.antsdb.saltedfish.server.fishnet.PacketFishRestore;
import com.antsdb.saltedfish.server.mysql.packet.AuthPacket;
import com.antsdb.saltedfish.sql.AuthPlugin;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.Session;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.sql.vdm.Checks;
import com.antsdb.saltedfish.sql.vdm.ObjectName;
import com.antsdb.saltedfish.sql.vdm.ShutdownException;
import com.antsdb.saltedfish.sql.vdm.Use;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.storage.KerberosHelper;
import com.antsdb.saltedfish.util.AntiCrashCrimeScene;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * represents a network session
 * 
 * @author *-xguo0<@
 */
public final class MysqlSession {
    static Logger _log = UberUtil.getThisLogger();
    static Logger _logBrokenSql = LoggerFactory.getLogger(MysqlSession.class.getName() + ".broken-sql");
    static AtomicInteger _threadId = new AtomicInteger(300);
    
    public static String SERVER_VERSION = Orca._version;
    public static byte PROTOCOL_VERSION = 0x0a;
    public static byte CHAR_SET_INDEX = 0x21;
    public static String AUTH_MYSQL_CLEARTEXT = "mysql_clear_password";
    public static String AUTH_MYSQL_NATIVE = "mysql_native_password";
    static final int MAX_PACKET_SIZE = 0xffffff;  
    
    ChannelWriter out;
    SaltedFish fish;
    Session session;
    PreparedStmtHandler preparedStmtHandler;
    int packetSequence;
    PacketEncoder encoder;
    private AuthPlugin authPlugin;
    private SocketAddress remote;
    private boolean isSslEnabled = false;
    private QueryHandler queryHandler;
    Map<Integer, MysqlPreparedStatement> prepared = new HashMap<>();
    private ByteBuffer bigPacket;
    private RestoreHandler restoreHandler;
    private boolean isUserSession;
    private ReplicationReceiver replicationReceiver;
    
    public MysqlSession(SaltedFish fish, ChannelWriter out, SocketAddress remote, boolean isUser) {
        this.out = out;
        this.fish = fish;
        this.encoder = new PacketEncoder(this);
        this.authPlugin = this.fish.getOrca().getAuthPlugin();
        this.remote = remote;
        this.queryHandler = new QueryHandler(this);
        this.preparedStmtHandler = new PreparedStmtHandler(this);
        this.isUserSession = isUser;
    }
   
    public void onConnect() {
        // status is set to SERVER_STATUS_AUTOCOMMIT 0x0002
        int status = 0x0002;
        this.encoder.writePacket(this.out, (packet) -> PacketEncoder.writeHandshakeBody(
                packet, 
                SERVER_VERSION,  
                PROTOCOL_VERSION, 
                _threadId.getAndIncrement(), 
                getServerCapabilities(), 
                CHAR_SET_INDEX, 
                status,
                this.authPlugin));
        this.out.flush();
    }

    /**
     * 
     * @param packet
     * @return 1: processed; 0: not processed; -1 switch to ssl; -2: close connection
     */
    public int run(ByteBuffer packet) {
        if (this.session != null && this.session.isClosed()) {
            return -2;
        }
        int result = 1;
        try {
            result = run0(packet);
            if (result == 0) {
                return result;
            }
        }
        catch (ShutdownException x) {
            throw x;
        }
        catch (ErrorMessage x) {
            writeErrMessage(x.getError(), x.toString());
        }
        catch (Throwable x) {
            _log.debug("error", x);
            if (x.getMessage() != null) {
                writeErrMessage(ERR_FOUND_EXCEPION, x.getMessage());
            }
            else {
                writeErrMessage(ERR_FOUND_EXCEPION, x.toString());
            }
        }
        finally {
            this.out.flush();
        }
        if (this.session != null && this.session.isClosed()) {
            return -2;
        }
        else {
            return result;
        }
    }

    private int run0(ByteBuffer input) throws Exception {
        if (this.session == null) {
            int sequence = input.get(3);
            this.encoder.packetSequence = sequence;
            return auth(input);
        }
        else {
            if (this.session.isClosed()) {
                throw new ErrorMessage(ER_ERROR_WHEN_EXECUTING_COMMAND, "session is closed");
            }
            ByteBuffer buf = null;
            try {
                this.session.notifyStartQuery();
                buf = handleBigPacket(input); 
                if (buf == null) {
                    return 1;
                }
                int sequence = buf.get(3);
                this.encoder.packetSequence = sequence;
                Packet packet = Packet.from(buf);
                return run1(buf, packet);
            }
            finally {
                this.session.notifyEndQuery();
                if ((this.bigPacket != null) && (buf == this.bigPacket)) {
                    MemoryManager.freeImmortal(AllocPoint.BIG_PACKET, this.bigPacket);
                    this.bigPacket = null;
                }
            }
        }
    }

    private int run1(ByteBuffer buf, Packet packet) throws Exception {
        long preMemory = MemoryManager.getThreadAllocation();
        try {
            return run2(buf, packet);
        }
        finally {
            long postMemory = MemoryManager.getThreadAllocation();
            if (preMemory != postMemory) {
                _log.warn("memory leak {}-{}: {}", preMemory, postMemory, StringUtils.left(packet.toString(), 200));
                /* for debug
                for (Exception i:MemoryManager.getTrace().values()) {
                    i.printStackTrace();
                }
                */
            }
        }
    }
    
    private int run2(ByteBuffer buf, Packet packet) throws Exception {
        if (packet instanceof PacketPing) {
            ping((PacketPing)packet);
            return 1;
        }
        else if (packet instanceof PacketDbInit) {
            init((PacketDbInit)packet);
            return 1;
        }
        else if (packet instanceof PacketQuery) {
            query((PacketQuery)packet, getDecoder());
            return 1;
        }
        else if (packet instanceof PacketQuit) {
            quit();
            return -2;
        }
        else if (packet instanceof PacketPrepare) {
            prepare((PacketPrepare)packet);
            return 1;
        }
        else if (packet instanceof PacketStmtClose) {
            closeStatement((PacketStmtClose)packet);
            return 1;
        }
        else if (packet instanceof PacketExecute) {
            buf.position(5);
            execute((PacketExecute)packet, buf);
            return 1;
        }
        else if (packet instanceof PacketFieldList) {
            fieldList((PacketFieldList)packet);
            return 1;
        }
        else if (packet instanceof PacketSetOption) {
            setOption((PacketSetOption)packet);
            return 1;
        }
        else if (packet instanceof PacketLongData) {
            setLongData((PacketLongData)packet);
            return 1;
        }
        else if (packet instanceof PacketFishBackup) {
            backup((PacketFishBackup)packet);
            return 1;
        }
        else if (packet instanceof PacketFishRestore) {
            restore((PacketFishRestore)packet);
            return 1;
        }
        else if (packet instanceof PacketFishRestoreStart) {
            restoreStart((PacketFishRestoreStart)packet);
            return 1;
        }
        else if (packet instanceof PacketFishRestoreEnd) {
            restoreEnd((PacketFishRestoreEnd)packet);
            return 1;
        }
        else if (packet instanceof PacketLogReplicate) {
            logReplicate((PacketLogReplicate)packet);
            return 1;
        }
        else {
            int cmd = buf.get(4) & 0xff;
            _log.error("unknown command {}", cmd);
            writeErrMessage(ER_ERROR_WHEN_EXECUTING_COMMAND, "unknown command " + cmd);
            return 0;
        }
    }

    /**
     * 
     * @param buf
     * @return true: big packet complete; false: not complete
     */
    private ByteBuffer handleBigPacket(ByteBuffer buf) {
        int size = buf.getInt(buf.position()) & 0xffffff;
        if (buf.remaining() != size + 4) {
            throw new IllegalArgumentException();
        }
        ByteBuffer result = null;
        if (this.bigPacket != null) {
            this.bigPacket = MemoryManager.growImmortal(
                    AllocPoint.BIG_PACKET, 
                    this.bigPacket, 
                    this.bigPacket.position() + size);
            buf.getInt();
            this.bigPacket.put(buf);
            if (size != MAX_PACKET_SIZE) {
                // end of the big packet.  
                result = this.bigPacket;
                result.flip();
                // update the sequene from the last big packet
                int sequence = buf.get(0x3) &  0xff;
                result.put(0x3, (byte)sequence);
                this.bigPacket = null;
            }
        }
        else {
            if (size == MAX_PACKET_SIZE) {
                // start of the big packet
                this.bigPacket = MemoryManager.allocImmortal(AllocPoint.BIG_PACKET, size + 4);
                this.bigPacket.order(ByteOrder.LITTLE_ENDIAN);
                this.bigPacket.put(buf);
            }
            else {
                result = buf;
            }
        }
        return result;
    }

    private void setLongData(PacketLongData packet) {
        MysqlPreparedStatement stmt = this.prepared.get(packet.getStatementId());
        if (stmt == null) {
            throw new ErrorMessage(ER_UNKNOWN_COM_ERROR, "statement id is not found: " + packet.getStatementId());
        }
        stmt.setLongData(packet.getParameterId(), packet.getDataAddress(), packet.getDataLength());
    }

    private void setOption(PacketSetOption packet) {
        throw new ErrorMessage(ER_UNKNOWN_COM_ERROR, "Field list unsupported!");
    }

    private void fieldList(PacketFieldList packet) {
        throw new ErrorMessage(ER_UNKNOWN_COM_ERROR, "Set option unsupported!");
    }

    private void execute(PacketExecute packet, ByteBuffer buf) {
        try {
            this.preparedStmtHandler.execute(buf);
        }
        catch (Exception x) {
            sqlError(x, this.preparedStmtHandler.getSql(packet.getStatementId()));
        }
    }

    private void closeStatement(PacketStmtClose packet) {
        this.preparedStmtHandler.close(packet.getStatementId());
    }

    private void prepare(PacketPrepare packet) throws SQLException {
        try {
            this.preparedStmtHandler.prepare(packet, getDecoder());
        }
        catch (Exception x) {
            sqlError(x, packet.getQuery(getDecoder()));
        }
    }

    private void quit() {
        this.session.close();
    }

    private void query(PacketQuery packet, Decoder decoder) throws Exception {
        ByteBuffer sql = packet.getQuery();
        AntiCrashCrimeScene.log(sql);
        /* for debug
        if (FakeResponse.fake(packet.getQueryAsCharBuf(decoder), this.out)) {
            return;
        }
        */
        /* for debug
         * MemoryManager.setTrace(true);
         */
        try {
            queryHandler.query(sql, decoder);
        }
        catch (Exception x) {
            sqlError(x, packet.getQueryAsString(getDecoder()));
        }
    }

    private void sqlError(Exception x, String sql) {
        int maxSqlSizeInLog = this.fish.getOrca().getHumpback().getConfig().getMaxSqlLengthInLog();
        _logBrokenSql.trace("broken sql: {}", StringUtils.left(sql, maxSqlSizeInLog));
        _logBrokenSql.trace("parser error", x);
        String msg = null;
        if (x instanceof ParseCancellationException) {
            if (x.getCause() instanceof InputMismatchException) {
                InputMismatchException xx = (InputMismatchException)x.getCause();
                String offend = xx.getOffendingToken().getText();
                msg = String.format("Invalid SQL. Offending token: %s", offend);
            }
            if (x.getCause() instanceof NoViableAltException) {
                NoViableAltException xx = (NoViableAltException)x.getCause();
                String offend = xx.getOffendingToken().getText();
                msg = String.format("Invalid SQL. Offending token: %s", offend);
            }
            if (msg == null) {
                msg = "You have an error in your SQL syntax";
            }
        }
        if (msg == null) {
            msg = x.getMessage();
            if (msg == null) {
                msg = x.toString();
            }
        }
        writeErrMessage(ERR_FOUND_EXCEPION, msg);
    }

    private void init(PacketDbInit request) {
        VdmContext vdm = new VdmContext(session, 0);
        new Use(request.getDbName()).run(vdm, null);
        this.out.write(PacketEncoder.OK_PACKET);
    }

    private void ping(PacketPing request) {
        this.encoder.writePacket(out, (packet)->{this.encoder.writeOKBody(packet, 0, 0, "yo", session);});
    }

    private int auth(ByteBuffer packet) {
        packet.getInt();
        AuthPacket request = new AuthPacket();
        request.read(packet);
        if (request.isSslEnabled() && !this.isSslEnabled) {
            this.isSslEnabled = true;
            return -1;
        }
        if (isKerberosEnable()) {
            if (!KerberosHelper.login(request.user, request.password)) {
                throw new ErrorMessage(ER_ACCESS_DENIED_ERROR, "Kerberos authentication failed");
            }
        }
        else if (!this.authPlugin.authenticate(request.user, request.passwordRaw)) {
            throw new ErrorMessage(ER_ACCESS_DENIED_ERROR, "Access denied for invalid user or password");
        }
        this.session = this.fish.getOrca().createSession(request.user, this.remote.toString(), this.isUserSession);
        this.session.setCurrentNamespace(request.dbname);
        _log.trace("Connection user:" + request.user);
        _log.trace("Connection default schema:" + request.dbname);
        this.out.write(request.isSslEnabled() ? PacketEncoder.SSL_AUTH_OK_PACKET : PacketEncoder.AUTH_OK_PACKET);
        this.out.flush();
        return 1;
    }

    byte getNextPacketSequence() {
        return (byte)++this.packetSequence;
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

    private boolean enableSSL() {
        String keyFile = this.fish.getConfig().getSSLKeyFile();
        String password = this.fish.getConfig().getSSLPassword();
        if (keyFile!=null || password!=null) {
            return true;
        }
        return false;
    }

    public boolean isKerberosEnable() {
        return this.fish.getConfig().isKerberosEnable() && this.fish.getConfig().isAuthEnable();
    }

    public void writeErrMessage(int errno, String msg) {
        if (msg == null) {
            msg = "";
        }
        ByteBuffer buf = Charset.defaultCharset().encode(msg);
        this.encoder.writePacket(this.out, (packet) -> this.encoder.writeErrorBody(
                packet, 
                errno, 
                buf));
    }

    public Map<Integer, MysqlPreparedStatement> getPrepared() {
        return this.prepared;
    }

    public void close() {
        if (this.encoder != null) {
            this.encoder.close();
        }
        if (this.session != null) {
            for (MysqlPreparedStatement i:this.prepared.values()) {
                i.close();
            }
            this.fish.getOrca().closeSession(this.session);
        }
    }

    private Decoder getDecoder() {
        if (this.session == null) {
            return Decoder.UTF8;
        }
        else {
            return this.session.getConfig().getRequestDecoder();
        }
    }
    
    private void backup(PacketFishBackup request) {
        ObjectName name = ObjectName.parse(request.getFullTableName());
        TableMeta table = Checks.tableExist(this.session, name);
        GTable gtable = this.session.getOrca().getHumpback().getTable(table.getId());
        long trxts = TrxMan.getNewVersion();
        for (RowIterator i = gtable.scan(0, trxts, true);;) {
            if (!i.next()) {
                break;
            }
            Row row = i.getRow();
            this.encoder.writePacket(out, (packet)->{
                this.encoder.writeRow(packet, row);
            });
        }
        this.encoder.writePacket(out, (packet)->{
            this.encoder.writeEOFBody(packet, session);
        });
    }

    private void restore(PacketFishRestore packet) {
        if (this.restoreHandler == null) {
            throw new OrcaException("no restore is in proceed");
        }
        this.restoreHandler.restore(packet);
    }
    
    private void restoreEnd(PacketFishRestoreEnd packet) {
        if (this.restoreHandler == null) {
            throw new OrcaException("no restore is in proceed");
        }
        this.restoreHandler = null;
        this.restoreHandler.end(packet);
    }

    private void restoreStart(PacketFishRestoreStart packet) {
        if (this.restoreHandler != null) {
            throw new OrcaException("restore is already in proceed");
        }
        this.restoreHandler = new RestoreHandler(this.session);
        this.restoreHandler.prepare(packet);
    }

    private void logReplicate(PacketLogReplicate request) throws Exception {
        if (this.replicationReceiver == null) {
            this.replicationReceiver = new ReplicationReceiver(this.session);
        }
        this.replicationReceiver.replicate(this, request);
    }

}
