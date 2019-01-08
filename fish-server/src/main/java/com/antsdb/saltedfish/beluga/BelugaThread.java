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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;

import com.antsdb.saltedfish.backup.ColumnBackupInfo;
import com.antsdb.saltedfish.backup.JdbcRestoreMain;
import com.antsdb.saltedfish.backup.TableBackupInfo;
import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.Replicator;
import com.antsdb.saltedfish.slave.DbUtils;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.Session;
import com.antsdb.saltedfish.sql.vdm.Cursor;
import com.antsdb.saltedfish.sql.vdm.Record;
import com.antsdb.saltedfish.util.UberFormatter;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author *-xguo0<@
 */
class BelugaThread extends Thread {
    static Logger _log = UberUtil.getThisLogger();
    
    Member member;
    private Orca orca;
    Replicator<PeerReplicator> replicator;
    PeerReplicator peer;

    BelugaThread(Orca orca, Member member) {
        super(member.endpoint);
        this.orca = orca;
        this.member = member;
        this.setDaemon(true);
    }

    @Override
    public void run() {
        try {
            run0();
        }
        catch (Exception x) {
            _log.error("error", x);
            this.member.setState(BelugaState.STOPPED, x.getMessage());
        }
    }

    private String getPrefix() {
        return "/" + this.orca.getHumpback().getServerId() + "/cluster/";
 
    }
    
    private void run0() throws Exception {
        if (this.member.init) {
            initSlave();
        }
        
        // start replication

        Humpback humpback = this.orca.getHumpback();
        this.peer = new PeerReplicator(humpback, this.member, getPrefix());
        this.replicator = new Replicator<>(getName(), humpback, this.peer, true);
        this.member.setState(BelugaState.LIVE, null);
        this.replicator.run();
    }

    private void initSlave() throws Exception {
        // initial load if necessary
        
        if (member.load) {
            load();
        }
        
        // done
        
        this.member.init = false;
    }

    private void load() throws Exception {
        this.member.setState(BelugaState.LOADING, null);
        JdbcRestoreMain restore = new JdbcRestoreMain(
                this.member.getHost(), 
                this.member.getPort(), 
                this.member.user, 
                this.member.password);
        restore.setThreads(this.member.nThreads);
        restore.setReplication(true);
        restore.connect();
        clean(restore.getConnection());
        int threads = member.nThreads <= 0 ? 4 : member.nThreads;
        restore.setThreads(threads);
        try {
            Session session = this.orca.createSystemSession();
            List<TableBackupInfo> tables = getTables(session);
            long start = System.currentTimeMillis();
            _log.info("start loading to {} with {} threads ...", this.member.endpoint, threads);
            for (TableBackupInfo table:tables) {
                _log.info("start loading {} ...", table.getFullName());
                this.member.setState(BelugaState.LOADING, "creating table " + table.getFullName() + " ...");
                restore.restoreDatabase(table);
                restore.restoreTable(table);
                long tableStart = System.currentTimeMillis();
                this.member.setState(BelugaState.LOADING, "restoring rows " + table.getFullName() + " ...");
                long rows = restoreContent(session, restore, table);
                _log.info("{}: {} rows {} rows/s", 
                          table.getFullName(), 
                          rows, UberUtil.throughput(tableStart, System.currentTimeMillis(), rows));
            }
            this.member.load = false;
            long end = System.currentTimeMillis();
            long elapse = end - start;
            long throught = (elapse == 0) ? 0 : restore.getRowsCount() * 1000 / elapse; 
            _log.info("load is completed with {} tables {} rows {} rows/s time {}", 
                      tables.size(), 
                      restore.getRowsCount(),
                      throught,
                      UberFormatter.time(elapse));
        }
        finally {
            restore.close();
        }
    }

    private void clean(Connection conn) throws SQLException {
        List<Map<String,Object>> databases = DbUtils.rows(conn, "SHOW DATABASES");
        for (Map<String,Object> row:databases) {
            String i = (String)row.values().iterator().next();
            if (i.equalsIgnoreCase("antsdb")) {
                continue;
            }
            if (i.equalsIgnoreCase("antsdb_")) {
                continue;
            }
            if (i.equals("information_schema")) {
                continue;
            }
            DbUtils.execute(conn, "DROP DATABASE " + i);
        }
    }

    private long restoreContent(Session session, JdbcRestoreMain restore, TableBackupInfo table) throws Exception {
        String sql = String.format("SELECT * FROM `%s`.`%s`", table.catalog, table.table);
        try (Cursor c = (Cursor)session.run(sql)) {
            return restore.restoreContent(c, table);
        }
    }
    
    private List<TableBackupInfo> getTables(Session session) throws SQLException  {
        List<TableBackupInfo> result = new ArrayList<>();
        List<String> databases = getDatabases(session);
        for (String i:databases) {
            if (i.equalsIgnoreCase("antsdb")) {
                continue;
            }
            if (i.equalsIgnoreCase("antsdb_")) {
                continue;
            }
            result.addAll(getTables(session, i));
        }
        return result;
    }
    
    private List<TableBackupInfo> getTables(Session session, String db) throws SQLException {
        List<TableBackupInfo> result = new ArrayList<>();
        String sql = "SHOW TABLES FROM " + db;
        try (Cursor rs = (Cursor)session.run(sql)) {
            for (;;) {
                long pRecord = rs.next();
                if (pRecord == 0) {
                    break;
                }
                String table = (String)Record.getValue(pRecord, 0);
                TableBackupInfo info = new TableBackupInfo();
                info.catalog = db;
                info.table = table;
                info.create = getCreateTable(session, info);
                info.columns = getcolumns(session, info);
                result.add(info);
            }
        }
        return result;
    }
    
    private List<String> getDatabases(Session session) throws SQLException {
        List<String> result = new ArrayList<>();
        try (Cursor cursor = (Cursor)session.run("show databases");) {
            for (;;) {
                long pRecord = cursor.next();
                if (pRecord == 0) {
                    break;
                }
                String name = (String)Record.getValue(pRecord, 0);
                result.add(name);
            }
        }
        return result;
    }
    
    private String getCreateTable(Session session, TableBackupInfo info) throws SQLException {
        String sql = String.format("SHOW CREATE TABLE `%s`.`%s`", info.catalog, info.table);
        try (Cursor rs = (Cursor)session.run(sql)) {
            long pRecord = rs.next();
            if (pRecord == 0) {
                throw new IllegalArgumentException();
            }
            String result = (String)Record.getValue(pRecord, 1);
            return result;
        }
    }

    private ColumnBackupInfo[] getcolumns(Session session, TableBackupInfo info) throws SQLException {
        List<ColumnBackupInfo> columns = new ArrayList<>();
        String sql = "SHOW COLUMNS FROM `" + info.catalog + "`.`" + info.table + "`";
        try (Cursor rs = (Cursor)session.run(sql)) {
            for (;;) {
                long pRecord = rs.next();
                if (pRecord == 0) {
                    break;
                }
                ColumnBackupInfo column = new  ColumnBackupInfo();
                column.name = (String)Record.getValue(pRecord, 0);
                columns.add(column);
            }
        }
        return columns.toArray(new ColumnBackupInfo[columns.size()]);
    }

}
