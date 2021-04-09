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
package com.antsdb.saltedfish.beluga;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Map;

import org.slf4j.Logger;

import com.antsdb.mysql.network.LogReplicateRequest;
import com.antsdb.mysql.network.MysqlClient;
import com.antsdb.mysql.network.PacketError;
import com.antsdb.saltedfish.nosql.Replicable;
import com.antsdb.saltedfish.nosql.ReplicationHandler2;
import com.antsdb.saltedfish.cpp.BluntHeap;
import com.antsdb.saltedfish.nosql.LogEntry;
import com.antsdb.saltedfish.nosql.Replicator;
import com.antsdb.saltedfish.server.mysql.ErrorMessage;
import com.antsdb.saltedfish.slave.DbUtils;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class PeerReplicator implements Replicable, ReplicationHandler2 {
    private static Logger _log = UberUtil.getThisLogger();
    
    Member member;
    MysqlClient client;
    ByteBuffer buf = ByteBuffer.allocateDirect(128);
    BluntHeap heap = new BluntHeap(buf);
    private Orca orca;
    private long lpLastStorage;
    
    public PeerReplicator(Orca orca, Member member, String prefix) {
        this.orca = orca;
        this.member = member;
    }

    @Override
    public void connect() throws Exception {
        // do nothing if connection is alive
        if (this.client != null) {
            if (this.client.ping()) {
                return;
            }
        }
        
        // verify server id
        String url = String.format("jdbc:mysql://%s", this.member.qnode.endpoint);
        try (Connection conn = DriverManager.getConnection(url)) {
            Map<String, Object> row = null;
            try {
                row = DbUtils.firstRow(conn, "SELECT 1 WHERE @@server_id=?", this.member.getServerId());
            }
            catch (Exception x) {
            }
            if (row == null) {
                throw new ErrorMessage(0, "unexpected server id from the slave node", this.member.getServerId());
            }
        }
        
        // connect
        if (this.client == null) {
            this.client = createConnection();
            _log.info("connection to {} is established", this.member.getEndpoint());
        }
        else if (!this.client.ping()) {
            this.client = createConnection();
            _log.info("connection to {} is resumed", this.member.getEndpoint());
        }
    }

    @Override
    public void flush(long lpRows, long lpIndexes) throws Exception {
        Replicator<Replicable> replicator = this.orca.getReplicator();
        if (replicator == null) {
            return;
        }
        long lpStorage = replicator.getReplicable().getCommittedLogPointer();
        if (this.lpLastStorage != lpStorage) {
            try (BluntHeap heap = new BluntHeap(128)) {
                StorageReplicationMark mark = StorageReplicationMark.alloc(heap);
                mark.setLogPointer(lpStorage);
                send(mark);
            }
            this.lpLastStorage = lpStorage;
        }
    }

    @Override
    public void all(long pEntry, long lpEntry) throws Exception {
        LogEntry entry = new LogEntry(lpEntry, pEntry);
        send(entry);
        this.member.lp = entry.getSpacePointer();
    }

    private void send(LogEntry entry) throws Exception {
        LogReplicateRequest request = new LogReplicateRequest(entry);
        this.client.send(request);
        ByteBuffer response = client.readPacket();
        if (client.isError(response)) {
            PacketError error = new PacketError(response);
            throw new Exception(error.getErrorMessage());
        }
    }
    
    public MysqlClient createConnection() throws Exception {
        MysqlClient client = new MysqlClient(this.member.getHost(), this.member.getPort());
        client.connect();
        client.login("", "");
        return client;
    }

    @Override
    public long getReplicateLogPointer() {
        return this.member.lp;
    }

    @Override
    public long getCommittedLogPointer() {
        return this.member.lp;
    }

    @Override
    public ReplicationHandler2 getReplayHandler() {
        return this;
    }
}
