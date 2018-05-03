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

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;

import com.antsdb.mysql.network.Packet;
import com.antsdb.mysql.network.PacketDbInit;
import com.antsdb.mysql.network.PacketExecute;
import com.antsdb.mysql.network.PacketFieldList;
import com.antsdb.mysql.network.PacketLongData;
import com.antsdb.mysql.network.PacketPing;
import com.antsdb.mysql.network.PacketPrepare;
import com.antsdb.mysql.network.PacketQuery;
import com.antsdb.mysql.network.PacketQuit;
import com.antsdb.mysql.network.PacketSetOption;
import com.antsdb.mysql.network.PacketStmtClose;
import com.antsdb.saltedfish.cpp.Bytes;
import com.antsdb.saltedfish.server.SaltedFish;
import com.antsdb.saltedfish.server.mysql.packet.AuthPacket;
import com.antsdb.saltedfish.sql.AuthPlugin;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.Session;
import com.antsdb.saltedfish.sql.vdm.ShutdownException;
import com.antsdb.saltedfish.sql.vdm.Use;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.storage.KerberosHelper;
import com.antsdb.saltedfish.util.UberUtil;

import static com.antsdb.saltedfish.server.mysql.MysqlConstant.*;
import static com.antsdb.saltedfish.server.mysql.util.MysqlErrorCode.*;

/**
 * represents a network session
 * 
 * @author *-xguo0<@
 */
public final class MysqlSession {
    static Logger _log = UberUtil.getThisLogger();
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
    
    public MysqlSession(SaltedFish fish, ChannelWriter out, SocketAddress remote) {
        this.out = out;
        this.fish = fish;
        this.encoder = new PacketEncoder(this);
        this.authPlugin = this.fish.getOrca().getAuthPlugin();
        this.remote = remote;
        this.queryHandler = new QueryHandler(this);
        this.preparedStmtHandler = new PreparedStmtHandler(this);
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
        catch (Exception x) {
            _log.debug("error", x);
            writeErrMessage(ERR_FOUND_EXCEPION, x.getMessage());
        }
        finally {
            this.out.flush();
        }
        return result;
    }

    private int run0(ByteBuffer input) throws Exception {
        if (this.session == null) {
            return auth(input);
        }
        else {
            if (this.session.isClosed()) {
                throw new OrcaException("session is closed");
            }
            try {
                this.session.notifyStartQuery();
                ByteBuffer buf = handleBigPacket(input); 
                if (buf == null) {
                    return 1;
                }
                int pos = buf.position();
                int sequence = buf.get(3);
                this.encoder.packetSequence = sequence;
                Packet packet = Packet.from(buf);
                if (packet instanceof PacketPing) {
                    ping((PacketPing)packet);
                    return 1;
                }
                else if (packet instanceof PacketDbInit) {
                    init((PacketDbInit)packet);
                    return 1;
                }
                else if (packet instanceof PacketQuery) {
                    query((PacketQuery)packet);
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
                    buf.position(pos + 5);
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
                else {
                    int cmd = buf.get(4);
                    _log.error("unknown command {}", cmd);
                    writeErrMessage(ER_ERROR_WHEN_EXECUTING_COMMAND, "unknown command " + cmd);
                    return 0;
                }
            }
            finally {
                this.session.notifyEndQuery();
            }
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
            this.bigPacket = grow(this.bigPacket, this.bigPacket.position() + size);
            buf.getInt();
            this.bigPacket.put(buf);
            if (size != MAX_PACKET_SIZE) {
                // end of the big packet
                result = this.bigPacket;
                result.flip();
                this.bigPacket = null;
            }
        }
        else {
            if (size == MAX_PACKET_SIZE) {
                // start of the big packet
                this.bigPacket = ByteBuffer.allocateDirect(size + 4);
                this.bigPacket.put(buf);
            }
            else {
                result = buf;
            }
        }
        return result;
    }

    private ByteBuffer grow(ByteBuffer buf, int size) {
        if (buf.capacity() >= size) {
            return buf;
        }
        ByteBuffer result = ByteBuffer.allocateDirect(size);
        buf.flip();
        result.put(buf);
        return result;
    }

    private void setLongData(PacketLongData packet) {
        MysqlPreparedStatement stmt = this.prepared.get(packet.getStatementId());
        if (stmt == null) {
            throw new ErrorMessage(ER_UNKNOWN_COM_ERROR, "statement id is not found: " + packet.getStatementId());
        }
        long pData = Bytes.allocSet(stmt.getHeap(), packet.getDataAddress(), packet.getDataLength());
        stmt.setParam(packet.getParameterId(), pData);
    }

    private void setOption(PacketSetOption packet) {
        throw new ErrorMessage(ER_UNKNOWN_COM_ERROR, "Field list unsupported!");
    }

    private void fieldList(PacketFieldList packet) {
        throw new ErrorMessage(ER_UNKNOWN_COM_ERROR, "Set option unsupported!");
    }

    private void execute(PacketExecute packet, ByteBuffer buf) {
        this.preparedStmtHandler.execute(buf);
    }

    private void closeStatement(PacketStmtClose packet) {
        this.preparedStmtHandler.close(packet.getStatementId());
    }

    private void prepare(PacketPrepare packet) throws SQLException {
        boolean success = false;
        try {
            this.preparedStmtHandler.prepare(packet);
            success = true;
        }
        finally {
            if (!success && _log.isDebugEnabled()) {
                _log.debug("borken sql:", StringUtils.left(packet.getQuery(), 1024));
            }
        }
    }

    private void quit() {
        this.session.close();
    }

    private void query(PacketQuery packet) throws Exception {
        boolean success = false;
        try {
            queryHandler.query(packet.getQueryAsCharBuf());
            success = true;
        }
        finally {
            if (!success && _log.isDebugEnabled()) {
                _log.debug("borken sql:", StringUtils.left(packet.getQuery(), 1024));
            }
        }
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
        this.session = this.fish.getOrca().createSession(request.user, this.remote.toString());
        this.session.setCurrentNamespace(request.dbname);
        if (_log.isTraceEnabled()) {
            _log.debug("Connection user:" + request.user);
            _log.debug("Connection default schema:" + request.dbname);
        }
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
        this.encoder.writePacket(this.out, (packet) -> this.encoder.writeErrorBody(
                packet, 
                errno, 
                Charset.defaultCharset().encode(msg)));
    }

    public Map<Integer, MysqlPreparedStatement> getPrepared() {
        return this.prepared;
    }

    public void close() {
        if (this.session != null) {
            for (MysqlPreparedStatement i:this.prepared.values()) {
                i.close();
            }
            this.fish.getOrca().closeSession(this.session);
        }
    }

}
