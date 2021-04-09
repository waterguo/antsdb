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

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;

import com.antsdb.saltedfish.util.Speedometer;
import com.antsdb.saltedfish.util.UberFormatter;
import com.antsdb.saltedfish.util.UberTime;
import com.antsdb.saltedfish.util.UberUtil;
import com.antsdb.saltedfish.nosql.DdlEntry;
import com.antsdb.saltedfish.nosql.DeleteRowEntry2;
import com.antsdb.saltedfish.nosql.HColumnRow;
import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.JdbcLog;
import com.antsdb.saltedfish.nosql.LogEntry;
import com.antsdb.saltedfish.nosql.Replicable;
import com.antsdb.saltedfish.nosql.ReplicationHandler2;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.SpaceManager;
import com.antsdb.saltedfish.nosql.SysMetaRow;
import com.antsdb.saltedfish.nosql.TableType;
import com.antsdb.saltedfish.nosql.TimestampEntry;
import com.antsdb.saltedfish.nosql.Gobbler.EntryType;

/**
 * replicates antsdb to a mysql slave
 * 
 * @author *-xguo0<@
 */
public abstract class JdbcReplicator implements Replicable, ReplicationHandler2 {
    private static Logger _log = UberUtil.getThisLogger();
    static JdbcLog _jdbclog;
    
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
    protected long lp;
    protected long lpCommited;
    private int opsSinceLastCommit;

    public JdbcReplicator(Humpback humpback, String host, String port, String user, String password) {
        if (_jdbclog == null) {
            initJdbcLog(humpback);
        }
        this.humpback = humpback;
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
    }

    private void initJdbcLog(Humpback humpback) {
        File file = new File(humpback.getHome(), "logs/jdbc-replication-log.dat");
        _jdbclog = new JdbcLog(file);
        try {
            _jdbclog.open(false);
        }
        catch (IOException x) {
            _log.warn("unable to open jdbc log", x);
        }
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
    public ReplicationHandler2 getReplayHandler() {
        return this;
    }

    @Override
    public void putRow(int tableId, long pRow, long version, long pEntry, long lpEntry) throws Exception {
        if (detectMetadataChange(tableId)) {
            return;
        }
        EntryType type = LogEntry.getType(pEntry);
        Row row = Row.fromMemoryPointer(pRow, version);
        if (_log.isTraceEnabled()) {
            _log.trace("{} {} {}", type, lpEntry, row.getKeySpec(tableId));
        }
        CudHandler handler = getHandler(tableId);
        boolean isBlobRow = false;
        if (handler.isBlobTable) {
            isBlobRow = true;
            handler = getHandler(tableId-1);
        }
        switch (type) {
        case INSERT2: {
            handler.insert(version, row, isBlobRow, lpEntry, tableId);
            this.ninserts++;
            break;
        }
        case UPDATE2:
        case PUT2: {
            handler.update(version, row, isBlobRow, lpEntry, tableId);
            this.nupdates++;
            break;
        }
        default:
            throw new IllegalArgumentException();
        }
        this.opsSinceLastCommit++;
        this.speedometer.sample(this.ninserts + this.nupdates + this.ndeletes);
        this.lp = lpEntry;
    }

    @Override
    public void deleteIndex(int tableId, long pKey, long version, long pEntry, long lpEntry) throws Exception {
        if (detectMetadataChange(tableId)) {
            return;
        }
        if (isIndex(tableId)) {
            return;
        }
        throw new IllegalArgumentException("user table must use deleteRow(): " + tableId);
    }

    @Override
    public void deleteRow(int tableId, long pKey, long version, long pEntry, long lpEntry) throws Exception {
        if (detectMetadataChange(tableId)) {
            return;
        }
        DeleteRowEntry2 entry = (DeleteRowEntry2) LogEntry.getEntry(lpEntry, pEntry);
        Row row = Row.fromMemoryPointer(entry.getRowPointer(), 0);
        if (_log.isTraceEnabled()) {
            _log.trace("delete {} {}", lpEntry, row.getKeySpec(tableId));
        }
        CudHandler handler = getHandler(tableId);
        if (!handler.isBlobTable) {
            handler.delete(row, lpEntry, tableId);
            this.opsSinceLastCommit++;
            this.ndeletes++;
            this.speedometer.sample(this.ninserts + this.nupdates + this.ndeletes);
        }
        this.lp = lpEntry;
    }

    @Override
    public long getCommittedLogPointer() {
        return this.lpCommited;
    }

    @Override
    public void flush(long lpRows, long lpIndexes) throws Exception {
        if (this.opsSinceLastCommit > 0) {
            String sql = "REPLACE INTO antsdb_.bookmarks VALUES (?, ?)";
            DbUtils.executeUpdate(this.conn, sql, getKey(), this.lp);
            boolean success = false;
            try {
                this.conn.commit();
                _log.trace("commit {} {}", this.lp, this.opsSinceLastCommit);
                this.lpCommited = this.lp;
                this.opsSinceLastCommit = 0;
                success = true;
            }
            finally {
                // if we failed at commit. we don't know how many rows have been updated. thus force 
                // re-read log pointer;
                if (!success) {
                    this.lpCommited = 0;
                    this.lp = 0;
                }
            }
        }
        else {
            this.lpCommited = this.lp;
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
    public void commit(long pEntry, long lpEntry) throws Exception {
        this.lp = lpEntry;
    }

    @Override
    public void timestamp(long pEntry, long lpEntry) {
        TimestampEntry entry = (TimestampEntry)LogEntry.getEntry(lpEntry, pEntry);
        this.latency = UberTime.getTime() - entry.getTimestamp();
    }

    @Override
    public void ddl(long pEntry, long lpEntry) throws Exception {
        // flush before ddl
        flush(lpEntry, 0);
        
        // ddl
        DdlEntry entry = (DdlEntry)LogEntry.getEntry(lpEntry, pEntry);
        String sql = entry.getDdl();
        int indexOfSemicolon = sql.indexOf(';');
        if (indexOfSemicolon > 0) {
            String sql1 = sql.substring(0, indexOfSemicolon);
            DbUtils.execute(this.conn, sql1);
        }
        DbUtils.execute(this.conn, sql.substring(indexOfSemicolon + 1));
        this.opsSinceLastCommit++;
        this.lp = entry.getSpacePointer();
        
        // flush after ddl
        flush(lpEntry, 0);
    }
    
    @Override
    public void transactionWindow(long pEntry, long lpEntry) throws Exception {
        // we want to flush here so that upstream handler can release resources used to track transaction.
        // such as TransactionalReplayer and BoblReorderReplayer
        this.opsSinceLastCommit++;
        flush(lpEntry, 0);
        this.lp = lpEntry;
    }
    
    @Override
    public void message(long pEntry, long lpEntry) throws Exception {
        this.lp = lpEntry;
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
            this.opsSinceLastCommit = 0;
            this.handlers.clear();
            _log.info("connection to {} is established", this.url);
        }
        else if (!DbUtils.ping(this.conn)) {
            this.conn = createConnection();
            this.opsSinceLastCommit = 0;
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
