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
package com.antsdb.saltedfish.slave;

import static com.antsdb.saltedfish.util.UberFormatter.capacity;
import static com.antsdb.saltedfish.util.UberFormatter.time;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;

import com.antsdb.saltedfish.nosql.Gobbler.CommitEntry;
import com.antsdb.saltedfish.nosql.Gobbler.DdlEntry;
import com.antsdb.saltedfish.nosql.Gobbler.DeleteEntry;
import com.antsdb.saltedfish.nosql.Gobbler.DeleteEntry2;
import com.antsdb.saltedfish.nosql.Gobbler.DeleteRowEntry;
import com.antsdb.saltedfish.nosql.Gobbler.DeleteRowEntry2;
import com.antsdb.saltedfish.nosql.Gobbler.IndexEntry;
import com.antsdb.saltedfish.nosql.Gobbler.IndexEntry2;
import com.antsdb.saltedfish.nosql.Gobbler.InsertEntry;
import com.antsdb.saltedfish.nosql.Gobbler.InsertEntry2;
import com.antsdb.saltedfish.nosql.Gobbler.MessageEntry;
import com.antsdb.saltedfish.nosql.Gobbler.MessageEntry2;
import com.antsdb.saltedfish.nosql.Gobbler.PutEntry;
import com.antsdb.saltedfish.nosql.Gobbler.PutEntry2;
import com.antsdb.saltedfish.nosql.Gobbler.TimestampEntry;
import com.antsdb.saltedfish.nosql.Gobbler.TransactionWindowEntry;
import com.antsdb.saltedfish.nosql.Gobbler.UpdateEntry;
import com.antsdb.saltedfish.nosql.Gobbler.UpdateEntry2;
import com.antsdb.saltedfish.util.Speedometer;
import com.antsdb.saltedfish.util.UberFormatter;
import com.antsdb.saltedfish.util.UberTime;
import com.antsdb.saltedfish.util.UberUtil;
import com.antsdb.saltedfish.nosql.HColumnRow;
import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.Replicable;
import com.antsdb.saltedfish.nosql.ReplicationHandler;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.SpaceManager;
import com.antsdb.saltedfish.nosql.SysMetaRow;
import com.antsdb.saltedfish.nosql.TableType;

/**
 * replicates antsdb to a mysql slave
 * 
 * @author *-xguo0<@
 */
public abstract class JdbcReplicator extends ReplicationHandler implements Replicable {
    private static Logger _log = UberUtil.getThisLogger();
    
    private Connection conn;
    protected Humpback humpback;
    private Map<Integer, CudHandler> handlers = new HashMap<>();
    private String url;
    private long ninserts;
    private long nupdates;
    private long ndeletes;
    private Speedometer speedometer = new Speedometer();
    private long latency;
    protected String host;
    protected String port;
    private String user;
    private String password;
    private long lp;
    private long lpCommited;

    public JdbcReplicator(Humpback humpback, String host, String port, String user, String password) {
        this.humpback = humpback;
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
    }

    public Connection createConnection() throws Exception {
        Class.forName("com.mysql.jdbc.Driver");
        Properties props = new Properties();
        props.setProperty("useServerPrepStmts","true");
        props.setProperty("zeroDateTimeBehavior","round");
        props.setProperty("user", this.user != null ? this.user : "");
        props.setProperty("password", this.password != null? this.password : "");
        this.url = String.format("jdbc:mysql://%s:%s", host, port);
        Connection result = DriverManager.getConnection(url, props);
        // we don't want mysql AUTO_INCREMENT during replication
        DbUtils.execute(result, "SET SESSION sql_mode='NO_AUTO_VALUE_ON_ZERO'");
        result.setAutoCommit(false);
        return result;
    }
    
    @Override
    public long getReplicateLogPointer() {
        return this.lpCommited;
    }

    @Override
    public ReplicationHandler getReplayHandler() {
        return this;
    }

    @Override
    public void insert(InsertEntry entry) throws Exception {
        insert(entry.getSpacePointer(), entry.getTrxId(), entry.getTableId(), entry.getRowPointer());
    }
    
    @Override
    public void insert(InsertEntry2 entry) throws Exception {
        insert(entry.getSpacePointer(), entry.getTrxId(), entry.getTableId(), entry.getRowPointer());
    }
    
    private void insert(long sp, long trxid, int tableId, long pRow) throws Exception {
        if (detectMetadataChange(tableId)) {
            return;
        }
        Row row = Row.fromMemoryPointer(pRow, 0);
        if (_log.isTraceEnabled()) {
            _log.trace("insert {} {}", sp, row.getKeySpec(tableId));
        }
        CudHandler handler = getHandler(tableId);
        boolean isBlobRow = false;
        if (handler.isBlobTable) {
            isBlobRow = true;
            handler = getHandler(tableId-1);
        }
        handler.insert(trxid, row, isBlobRow);
        this.ninserts++;
        this.speedometer.sample(this.ninserts + this.nupdates + this.ndeletes);
        this.lp = sp;
    }

    @Override
    public void update(UpdateEntry entry) throws Exception {
        update(entry.getSpacePointer(), entry.getTrxId(), entry.getTableId(), entry.getRowPointer());
    }
    
    @Override
    public void update(UpdateEntry2 entry) throws Exception {
        update(entry.getSpacePointer(), entry.getTrxId(), entry.getTableId(), entry.getRowPointer());
    }
    
    private void update(long sp, long trxid, int tableId, long pRow) throws Exception {
        if (detectMetadataChange(tableId)) {
            return;
        }
        Row row = Row.fromMemoryPointer(pRow, 0);
        if (_log.isTraceEnabled()) {
            _log.trace("update {} {}", sp, row.getKeySpec(tableId));
        }
        CudHandler handler = getHandler(tableId);
        boolean isBlobRow = false;
        if (handler.isBlobTable) {
            isBlobRow = true;
            handler = getHandler(tableId-1);
        }
        handler.update(trxid, row, isBlobRow);
        this.nupdates++;
        this.speedometer.sample(this.ninserts + this.nupdates + this.ndeletes);
        this.lp = sp;
    }

    @Override
    public void put(PutEntry entry) throws Exception {
        put(entry.getSpacePointer(), entry.getTrxId(), entry.getTableId(), entry.getRowPointer());
    }
    
    @Override
    public void put(PutEntry2 entry) throws Exception {
        put(entry.getSpacePointer(), entry.getTrxId(), entry.getTableId(), entry.getRowPointer());
    }
    
    private void put(long sp, long trxid, int tableId, long pRow) throws Exception {
        if (detectMetadataChange(tableId)) {
            return;
        }
        Row row = Row.fromMemoryPointer(pRow, 0);
        CudHandler handler = getHandler(tableId);
        boolean isBlobRow = false;
        if (handler.isBlobTable) {
            isBlobRow = true;
            handler = getHandler(tableId-1);
        }
        handler.update(trxid, row, isBlobRow);
        this.lp = sp;
    }

    @Override
    public void delete(DeleteEntry entry) throws Exception {
        delete(entry.getTableId());
    }
    
    @Override
    public void delete(DeleteEntry2 entry) throws Exception {
        delete(entry.getTableId());
    }
    
    private void delete(int tableId) throws Exception {
        if (detectMetadataChange(tableId)) {
            return;
        }
        if (isIndex(tableId)) {
            return;
        }
        throw new IllegalArgumentException("user table must use deleteRow(): " + tableId);
    }

    @Override
    public void deleteRow(DeleteRowEntry entry) throws Exception {
        deleteRow(entry.getSpacePointer(), entry.getTableId(), entry.getRowPointer());
    }
    
    @Override
    public void deleteRow(DeleteRowEntry2 entry) throws Exception {
        deleteRow(entry.getSpacePointer(), entry.getTableId(), entry.getRowPointer());
    }
    
    private void deleteRow(long sp, int tableId, long pRow) throws Exception {
        if (detectMetadataChange(tableId)) {
            return;
        }
        Row row = Row.fromMemoryPointer(pRow, 0);
        if (_log.isTraceEnabled()) {
            _log.trace("delete {} {}", sp, row.getKeySpec(tableId));
        }
        CudHandler handler = getHandler(tableId);
        if (!handler.isBlobTable) {
            handler.delete(row);
            this.ndeletes++;
            this.speedometer.sample(this.ninserts + this.nupdates + this.ndeletes);
        }
        this.lp = sp;
    }

    
    @Override
    public void index(IndexEntry entry) throws Exception {
    }

    @Override
    public void index(IndexEntry2 entry) throws Exception {
    }

    @Override
    public long getCommittedLogPointer() {
        return this.lpCommited;
    }

    @Override
    public void flush() throws Exception {
        if (this.lp != this.lpCommited) {
            String sql = "REPLACE INTO antsdb_.bookmarks VALUES (?, ?)";
            DbUtils.executeUpdate(this.conn, sql, getKey(), this.lp);
            boolean success = false;
            try {
                this.conn.commit();
                success = true;
            }
            finally {
                // if we failed at commit. we don't know how many rows have been updated. thus force 
                // re-read log pointer;
                if (success) {
                    this.lpCommited = this.lp;
                }
                else {
                    this.lpCommited = 0;
                    this.lp = 0;
                }
            }
        }
    }

    private boolean detectMetadataChange(int tableId) {
        switch (tableId) {
        case Humpback.SYSMETA_TABLE_ID:
        case Humpback.SYSNS_TABLE_ID:
        case Humpback.SYSCOLUMN_TABLE_ID:
            this.handlers.clear();
        }
        return tableId < 0x100;
    }

    @Override
    public void commit(CommitEntry entry) throws Exception {
        this.lp = entry.getSpacePointer();
    }

    @Override
    public void timestamp(TimestampEntry entry) {
        this.latency = UberTime.getTime() - entry.getTimestamp();
    }

    @Override
    public void ddl(DdlEntry entry) throws Exception {
        String sql = entry.getDdl();
        int indexOfSemicolon = sql.indexOf(';');
        if (indexOfSemicolon > 0) {
            DbUtils.execute(this.conn, sql.substring(0, indexOfSemicolon));
        }
        DbUtils.execute(this.conn, sql.substring(indexOfSemicolon + 1));
        flush();
        this.lp = entry.getSpacePointer();
        flush();
    }
    
    @Override
    public void transactionWindow(TransactionWindowEntry entry) throws Exception {
        // we want to flush here so that upstream handler can release resources used to track transaction.
        // such as TransactionalReplayer and BoblReorderReplayer
        flush();
        this.lp = entry.getSpacePointer();
    }
    
    @Override
    public void message(MessageEntry entry) throws Exception {
        this.lp = entry.getSpacePointer();
    }

    @Override
    public void message(MessageEntry2 entry) throws Exception {
        this.lp = entry.getSpacePointer();
    }

    private boolean isIndex(int tableId) {
        SysMetaRow info = this.humpback.getTableInfo(tableId);
        return info.getType() == TableType.INDEX;
    }
    
    private CudHandler getHandler(int tableId) throws Exception {
        CudHandler result = this.handlers.get(tableId);
        if (result == null) {
            result = createHandler(tableId);
            this.handlers.put(tableId, result);
        }
        return result;
    }

    private CudHandler createHandler(int tableId) throws SQLException {
        SysMetaRow table = this.humpback.getTableInfo(tableId);
        List<HColumnRow> columns = this.humpback.getColumns(tableId);
        CudHandler result = new CudHandler();
        result.prepare(this.conn, table, columns);
        return result;
    }

    public Map<String, Object> getSummary() {
        Map<String, Object> props = new HashMap<>();
        props.put("total inserts", this.ninserts);
        props.put("total updates", this.nupdates);
        props.put("total deletes", this.ndeletes);
        props.put("ops/second", this.speedometer.getSpeed());
        props.put("latency", time(this.latency));
        props.put("log pointer", UberFormatter.hex(this.lpCommited));
        props.put("pending data", capacity(getPendingBytes()));
        return props;
    }
    
    private long getPendingBytes() {
        SpaceManager sm = this.humpback.getSpaceManager();
        long latest = sm.getAllocationPointer();
        long current = this.lp;
        long size = sm.minus(latest, current);
        return size;
    }

    protected Connection getConnection() throws Exception {
        return this.conn;
    }
    
    @Override
    public void connect() throws Exception {
        if (conn == null) {
            this.conn = createConnection();
            this.handlers.clear();
            _log.info("connection to {} is established", this.url);
        }
        else if (!DbUtils.ping(this.conn)) {
            this.conn = createConnection();
            this.handlers.clear();
            _log.info("connection to {} is resumed", this.url);
        }
        if (this.lpCommited == 0) {
            Map<String,Object> row = DbUtils.firstRow(conn, "SELECT * FROM antsdb_.bookmarks WHERE name=?", getKey());
            this.lp = Long.parseLong((String)row.get("value"));
            this.lpCommited = this.lp;
        }
    }

    private Object getKey() {
        return this.humpback.getServerId() + "/lp";
    }
    
    public static void setup(Connection conn, long serverId, long lp) throws SQLException {
        DbUtils.execute(conn, "CREATE DATABASE IF NOT EXISTS antsdb_");
        DbUtils.execute(conn, "CREATE TABLE IF NOT EXISTS antsdb_.bookmarks (" + 
                "name varchar(100) NOT NULL PRIMARY KEY," +
                "value varchar(300) DEFAULT NULL) ENGINE=InnoDB;");
        String key = serverId + "/lp";
        DbUtils.executeUpdate(conn, "REPLACE INTO antsdb_.bookmarks (name,value) VALUES (?,?)", key, lp);
    }
    
    @Override
    public String toString() {
        return this.host + ":" + port;
    }
}
