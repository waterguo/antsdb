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

import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;

import com.antsdb.saltedfish.nosql.Gobbler.CommitEntry;
import com.antsdb.saltedfish.nosql.Gobbler.DeleteEntry;
import com.antsdb.saltedfish.nosql.Gobbler.DeleteRowEntry;
import com.antsdb.saltedfish.nosql.Gobbler.InsertEntry;
import com.antsdb.saltedfish.nosql.Gobbler.MessageEntry;
import com.antsdb.saltedfish.nosql.Gobbler.PutEntry;
import com.antsdb.saltedfish.nosql.Gobbler.TimestampEntry;
import com.antsdb.saltedfish.nosql.Gobbler.TransactionWindowEntry;
import com.antsdb.saltedfish.nosql.Gobbler.UpdateEntry;
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
public class SlaveReplicator extends ReplicationHandler implements Replicable {
    private static Logger _log = UberUtil.getThisLogger();
    
    private Connection conn;
    private long sp;
    private Humpback humpback;
    private Map<Integer, CudHandler> handlers = new HashMap<>();
    private String url;
    private long ninserts;
    private long nupdates;
    private long ndeletes;
    private Speedometer speedometer = new Speedometer();
    private long latency;
    private long commitedLp;

    public SlaveReplicator(Humpback humpback) throws Exception {
        this.humpback = humpback;
        open();
    }

    private void open() throws Exception {
        connect();
        Map<String, Object> row = DbUtils.firstRow(this.conn, "SHOW TABLES FROM antsdb LIKE 'antsdb_slave'");
        if (row == null) {
            throw new SQLException("antsdb_slave is not found in slave database");
        }
        Properties props = DbUtils.properties(this.conn, "SELECT * FROM antsdb.antsdb_slave");
        long defaultsp = this.humpback.getGobbler().getLatestSp();
        this.sp = Long.parseLong(props.getProperty("sp", String.valueOf(defaultsp)));
        this.commitedLp = this.sp;
        _log.info("slave replication starts from {}", this.sp);
    }
    
    private void connect() throws Exception {
        this.conn = getConnection();
    }
    
    Connection getConnection() throws Exception {
        Class.forName("com.mysql.jdbc.Driver");
        this.url = this.humpback.getConfig().getSlaveUrl();
        String user = this.humpback.getConfig().getSlaveUser();
        String password = this.humpback.getConfig().getSlavePassword();
        Connection result = DriverManager.getConnection(url, user, password);
        // we don't want mysql AUTO_INCREMENT during replication
        DbUtils.execute(result, "SET SESSION sql_mode='NO_AUTO_VALUE_ON_ZERO'");
        return result;
    }
    
    @Override
    public long getReplicateLogPointer() {
        return this.sp;
    }

    @Override
    public ReplicationHandler getReplayHandler() {
        return this;
    }

    @Override
    public void putRows(int tableId, List<Long> rows) {
        throw new NotImplementedException();
    }

    @Override
    public void putIndexLines(int tableId, List<Long> indexLines) {
        throw new NotImplementedException();
    }

    @Override
    public void deletes(int tableId, List<Long> deletes) {
        throw new NotImplementedException();
    }

    @Override
    public void flush() throws Exception {
        String sql = "REPLACE antsdb.antsdb_slave VALUES ('sp', ?)";
        DbUtils.executeUpdate(this.conn, sql, this.sp);
        this.commitedLp = this.sp;
    }
    
    @Override
    public void insert(InsertEntry entry) throws Exception {
        int tableId = entry.getTableId();
        if (detectMetadataChange(tableId)) {
            return;
        }
        long pRow = entry.getRowPointer();
        Row row = Row.fromMemoryPointer(pRow, 0);
        if (_log.isTraceEnabled()) {
            _log.trace("insert {} {}", entry.getSpacePointer(), row.getKeySpec(entry.getTableId()));
        }
        CudHandler handler = getHandler(tableId);
        boolean isBlobRow = false;
        if (handler.isBlobTable) {
            isBlobRow = true;
            handler = getHandler(tableId-1);
        }
        handler.insert(entry.getTrxId(), row, isBlobRow);
        this.ninserts++;
        this.speedometer.sample(this.ninserts + this.nupdates + this.ndeletes);
        this.sp = entry.getSpacePointer();
    }

    @Override
    public void update(UpdateEntry entry) throws Exception {
        int tableId = entry.getTableId();
        if (detectMetadataChange(tableId)) {
            return;
        }
        long pRow = entry.getRowPointer();
        Row row = Row.fromMemoryPointer(pRow, 0);
        if (_log.isTraceEnabled()) {
            _log.trace("update {} {}", entry.getSpacePointer(), row.getKeySpec(entry.getTableId()));
        }
        CudHandler handler = getHandler(tableId);
        boolean isBlobRow = false;
        if (handler.isBlobTable) {
            isBlobRow = true;
            handler = getHandler(tableId-1);
        }
        handler.update(entry.getTrxId(), row, isBlobRow);
        this.nupdates++;
        this.speedometer.sample(this.ninserts + this.nupdates + this.ndeletes);
        this.sp = entry.getSpacePointer();
    }

    @Override
    public void put(PutEntry entry) throws Exception {
        int tableId = entry.getTableId();
        if (detectMetadataChange(tableId)) {
            return;
        }
        long pRow = entry.getRowPointer();
        Row row = Row.fromMemoryPointer(pRow, 0);
        CudHandler handler = getHandler(tableId);
        boolean isBlobRow = false;
        if (handler.isBlobTable) {
            isBlobRow = true;
            handler = getHandler(tableId-1);
        }
        handler.update(entry.getTrxId(), row, isBlobRow);
        this.sp = entry.getSpacePointer();
    }

    @Override
    public void delete(DeleteEntry entry) throws Exception {
        int tableId = entry.getTableId();
        if (detectMetadataChange(tableId)) {
            return;
        }
        if (isIndex(tableId)) {
            return;
        }
        throw new IllegalArgumentException("user table must use deleteRow(): " + entry.getTableId());
    }

    @Override
    public void deleteRow(DeleteRowEntry entry) throws Exception {
        int tableId = entry.getTableId();
        if (detectMetadataChange(tableId)) {
            return;
        }
        long pRow = entry.getRowPointer();
        Row row = Row.fromMemoryPointer(pRow, 0);
        if (_log.isTraceEnabled()) {
            _log.trace("delete {} {}", entry.getSpacePointer(), row.getKeySpec(entry.getTableId()));
        }
        CudHandler handler = getHandler(tableId);
        if (!handler.isBlobTable) {
            handler.delete(row);
            this.ndeletes++;
            this.speedometer.sample(this.ninserts + this.nupdates + this.ndeletes);
        }
        this.sp = entry.getSpacePointer();
    }

    private boolean detectMetadataChange(int tableId) {
        if (tableId < 0x100) {
            if (tableId != 0x50) {
                // 0x50 is sequence table
                this.handlers.clear();
            }
            return true;
        }
        else {
            return false;
        }
    }

    @Override
    public void commit(CommitEntry entry) throws Exception {
        this.sp = entry.getSpacePointer();
    }

    @Override
    public void timestamp(TimestampEntry entry) {
        this.latency = UberTime.getTime() - entry.getTimestamp();
    }

    @Override
    public void message(MessageEntry entry) throws Exception {
        String msg = entry.getMessage();
        if (msg.startsWith("!!")) {
            String sql = msg.substring(2);
            int indexOfSemicolon = sql.indexOf(';');
            if (indexOfSemicolon > 0) {
                DbUtils.execute(this.conn, sql.substring(0, indexOfSemicolon));
            }
            DbUtils.execute(this.conn, sql.substring(indexOfSemicolon + 1));
        }
        this.sp = entry.getSpacePointer();
    }

    @Override
    public void transactionWindow(TransactionWindowEntry entry) throws Exception {
        this.sp = entry.getSpacePointer();
    }
    
    private boolean isIndex(int tableId) {
        SysMetaRow info = this.humpback.getTableInfo(tableId);
        return info.getType() == TableType.INDEX;
    }
    
    private CudHandler getHandler(int tableId) throws Exception {
        if (this.conn == null) {
            this.conn = getConnection();
            _log.info("connection to {} is resumed", this.url);
        }
        if (!DbUtils.ping(this.conn)) {
            _log.info("connection to {} is lost", this.url);
            this.conn = null;
            connect();
        }
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
        props.put("log pointer", UberFormatter.hex(this.sp));
        props.put("pending data", capacity(getPendingBytes()));
        return props;
    }
    
    private long getPendingBytes() {
        SpaceManager sm = this.humpback.getSpaceManager();
        long latest = sm.getAllocationPointer();
        long current = this.sp;
        long size = sm.minus(latest, current);
        return size;
    }

    @Override
    public long getCommittedLogPointer() {
        return this.commitedLp;
    }
    
}
