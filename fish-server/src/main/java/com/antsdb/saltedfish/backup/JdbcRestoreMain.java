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
package com.antsdb.saltedfish.backup;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Properties;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import org.apache.commons.cli.Options;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.BetterCommandLine;
import com.antsdb.saltedfish.slave.DbUtils;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.vdm.Cursor;
import com.antsdb.saltedfish.sql.vdm.Record;
import com.antsdb.saltedfish.util.IOUtils;
import com.antsdb.saltedfish.util.MysqlJdbcUtil;
import com.antsdb.saltedfish.util.UberFormatter;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class JdbcRestoreMain extends BetterCommandLine {
    static final Logger _log = UberUtil.getThisLogger();
    
    private String host;
    private String port;
    private Connection conn;
    private long nRows;
    private long start;
    private long end;
    private String user;
    private String password;
    private int batchSize;
    private RestoreThreadPool pool;
    private int nThreads = 4;
    private String rename;
    private boolean skipCreateTable;
    private boolean ignoreError;
    private int nErrors;

    private String filter;

    public JdbcRestoreMain() {
    }
    
    public JdbcRestoreMain(String host, int port, String user, String password) {
        this.host = host;
        this.port = String.valueOf(port);
        this.user = user;
        this.password = password;
    }

    public static void main(String[] args) throws Exception {
        MysqlJdbcUtil.loadClass();
        new JdbcRestoreMain().parseAndRun(args);
    }

    @Override
    protected void buildOptions(Options options) {
        options.addOption(null, "file",  true, "dump file");
        options.addOption(null, "host", true, "host");
        options.addOption(null, "port", true, "port");
        options.addOption(null, "user", true, "user");
        options.addOption(null, "password", true, "password");
        options.addOption(null, "batch-size", true, "jdbc batch size");
        options.addOption(null, "auto-commit", false, "use auto-commit or not");
        options.addOption(null, "threads", true, "number of threads");
        options.addOption(null, "rename", true, "rename namespace <new name>=<old name>");
        options.addOption(null, "skip-create-table", false, "skip create table statement. use truncate instead");
        options.addOption(null, "ignore-error", false, "igonre errors");
        options.addOption("u", "user", true, "user");
        options.addOption("p", "password", true, "password");
        options.addOption(null, "filter", true, "table filter using java patterns");
    }

    @Override
    protected void run() throws Exception {
        this.host = this.cmdline.getOptionValue("host", "localhost");
        this.port = this.cmdline.getOptionValue("port", "3306");
        this.user = this.cmdline.getOptionValue("user", "");
        this.password = this.cmdline.getOptionValue("password", "");
        this.batchSize = Integer.parseInt(this.cmdline.getOptionValue("batch-size", "100"));
        this.nThreads = Integer.parseInt(this.cmdline.getOptionValue("threads", "4"));
        this.rename = this.cmdline.getOptionValue("rename");
        this.skipCreateTable = this.cmdline.hasOption("skip-create-table");
        this.ignoreError = this.cmdline.hasOption("ignore-error");
        this.filter = this.cmdline.getOptionValue("filter");
        
        InputStream in;
        if (this.cmdline.hasOption("file")) {
            in = new FileInputStream(this.cmdline.getOptionValue("file"));
        }
        else {
            in = System.in;
        }
        
        connect();
        run(new BufferedInputStream(in, 1024 * 1024 * 16));
    }
        
    private void run(InputStream in) throws Exception {
        println("number of threads: %d", this.nThreads);
        println("batch-size: %d", this.batchSize);
        if (this.rename != null) {
            println("rename: %s", this.rename);
        }
        DataInputStream din = new DataInputStream(in);
        BackupFile backup = BackupFile.open(in);
        this.start = System.currentTimeMillis();
        rename(backup);
        for (TableBackupInfo table:backup.tables) {
            boolean pass = isTableFiltered(table);
            if (!pass) {
                System.out.print("restoring " + table.getFullName() + " .");
                restoreDatabase(table);
                System.out.print(".");
                restoreTable(table);
                System.out.print(".");
            }
            this.conn.setAutoCommit(false);
            restoreContent(din, table, pass);
            this.conn.commit();
        }
        this.end = System.currentTimeMillis();
        report(backup);
    }

    private boolean isTableFiltered(TableBackupInfo table) {
        if (table.catalog.equalsIgnoreCase("antsdb_")) {
            return true;
        }
        if (this.filter == null) {
            return false;
        }
        Pattern ptn = Pattern.compile(this.filter.replace(".","\\.").replace("*",".*"));
        return !ptn.matcher(table.getFullName()).matches();
    }

    private void rename(BackupFile backup) {
        if (this.rename == null) {
            return;
        }
        String[] temp = StringUtils.split(this.rename, '=');
        if (temp.length != 2) {
            println("invalid rename pattern: " + this.rename);
            System.exit(-1);
        }
        String newname = temp[0];
        String oldname = temp[1];
        for (TableBackupInfo i:backup.tables) {
            if (i.catalog.equals(oldname)) {
                i.catalog = newname;
            }
        }
    }

    private void report(BackupFile backup) {
        println("total tables: %d", backup.tables.size());
        println("total rows: %d", this.nRows);
        println("time: %s", UberFormatter.time(this.end - this.start));
        println("throughput: %d rows/s", getThroughput());
        if (this.nErrors != 0) {
            println("errors: %d", this.nErrors);
        }
    }

    public void restoreContent(DataInputStream din, TableBackupInfo table, boolean pass) throws Exception {
        din.readUTF();
        Supplier<Object[]> rowReader = ()-> {
            try {
                din.mark(4);
                if (din.readInt() == 0) {
                    return null;
                }
                din.reset();
                Object[] row = new Object[table.columns.length];
                for (int i=0; i<table.columns.length; i++) {
                    row[i] = readValue(din);
                }
                return row;
            }
            catch (Exception x) {
                throw new RuntimeException(x);
            }
        };
        if (pass) {
            skipContent(table, rowReader);
        }
        else {
            long count = restoreContent(table, rowReader);
            println(" " + count + " rows");
        }
    }

    public long restoreContent(Cursor c, TableBackupInfo table) throws Exception {
        long count = restoreContent(table, ()-> {
            try {
                long pRecord = c.next();
                if (pRecord == 0) {
                    return null;
                }
                Object[] row = new Object[table.columns.length];
                for (int i=0; i<table.columns.length; i++) {
                    row[i] = Record.getValue(pRecord, i);
                }
                return row;
            }
            catch (Exception x) {
                throw new RuntimeException(x);
            }
        });
        return count;
    }
    
    private void skipContent(TableBackupInfo table, Supplier<Object[]> call) {
        println("skipping table %s ...", table.getFullName());
        for (;;) {
            Object[] row = call.get();
            if (row == null) {
                break;
            }
        }
    }

    public long restoreContent(TableBackupInfo table, Supplier<Object[]> reader) 
    throws Exception {
        // prepare sql
        
        this.pool.clear();
        InsertBuilder builder = new InsertBuilder();
        builder.catalog = table.catalog;
        builder.table = table.table;
        for (ColumnBackupInfo column:table.columns) {
            builder.columns.add(column.name);
        }
        String sql = builder.toString();
        this.pool.prepare(sql);
        
        // send rows to restore threads  

        long count;
        try {
            for (;;) {
                Object[] row = reader.get();
                if (row == null) {
                    break;
                }
                this.pool.send(row);
            }
            this.pool.flush();
            this.pool.waitForCompletion();
        }
        catch (Exception x) {
            if (this.ignoreError) {
                print(x.getMessage());
                this.nErrors++;
                UberUtil.sleep(1000);
            }
            else {
                throw x;
            }
        }
        count = this.pool.getCount(); 
        this.nRows += count;
        
        return count;
    }

    public void restoreTable(TableBackupInfo table) throws SQLException {
        try {
            DbUtils.execute(conn, "USE " + table.catalog);
            if (this.skipCreateTable) {
                DbUtils.execute(conn, "TRUNCATE `" + table.table + "`");
            }
            else {
                DbUtils.execute(conn, "DROP TABLE IF EXISTS `" + table.table + "`");
                DbUtils.execute(conn, table.create);
            }
        }
        catch (Exception x) {
            throw new OrcaException(x, "unable to restore table {} : {}", table.getFullName(), x.getMessage()); 
        }
    }

    public void restoreDatabase(TableBackupInfo table) throws SQLException {
        String sql = "CREATE DATABASE IF NOT EXISTS " + table.catalog;
        DbUtils.execute(conn, sql);
    }

    private Object readValue(DataInputStream din) throws IOException {
        Object result = null;
        int type = din.readByte();
        switch (type) {
        case JdbcBackupMain.TYPE_BIG_INTEGER:
            result = new BigInteger(din.readUTF());
            break;
        case JdbcBackupMain.TYPE_BYTES:
            result = readBytes(din);
            break;
        case JdbcBackupMain.TYPE_DATE:
            result = new Date(din.readLong());
            break;
        case JdbcBackupMain.TYPE_DECIMAL:
            result = new BigDecimal(din.readUTF());
            break;
        case JdbcBackupMain.TYPE_DOUBLE:
            result = din.readDouble();
            break;
        case JdbcBackupMain.TYPE_FLOAT:
            result = din.readFloat();
            break;
        case JdbcBackupMain.TYPE_INTEGER:
            result = din.readInt();
            break;
        case JdbcBackupMain.TYPE_LONG:
            result = din.readLong();
            break;
        case JdbcBackupMain.TYPE_NULL:
            break;
        case JdbcBackupMain.TYPE_STRING:
            result = IOUtils.readUtf(din);
            break;
        case JdbcBackupMain.TYPE_TIME:
            result = new Time(din.readLong());
            break;
        case JdbcBackupMain.TYPE_TIMESTAMP:
            result = new Timestamp(din.readLong());
            break;
        case JdbcBackupMain.TYPE_BOOL:
            result = din.readBoolean();
            break;
        default:
            throw new IllegalArgumentException(String.valueOf(type));
        }
        return result;
    }

    private Object readBytes(DataInputStream din) throws IOException {
        int length = (int)IOUtils.readCompactLong(din);
        byte[] result = new byte[length];
        din.readFully(result);
        return result;
    }

    public Connection createConnection() throws SQLException {
        String url = MysqlJdbcUtil.getUrl(this.host, Integer.parseInt(this.port), null);
        Properties props = new Properties();
        props.setProperty("user", this.user==null ? "" : this.user);
        props.setProperty("password", this.password==null ? "" : this.password);
        props.setProperty("useServerPrepStmts", "true");
        Connection conn = DriverManager.getConnection(url, props);
        DbUtils.execute(conn, "SET FOREIGN_KEY_CHECKS=0");
        DbUtils.execute(conn, "SET UNIQUE_CHECKS=0");
        DbUtils.execute(conn, "SET SQL_MODE='NO_AUTO_VALUE_ON_ZERO'");
        try {
            DbUtils.execute(conn, "SET @@antsdb_slave_replication_session='true'");
        }
        catch (SQLException ignored) {
        }
        return conn;
    }
    
    public void connect() throws SQLException {
        this.conn = createConnection();
        this.pool = new RestoreThreadPool(this.nThreads, this.batchSize);
        this.pool.start(()-> {
            try {
                return createConnection();
            }
            catch (SQLException x) {
                throw new RuntimeException(x);
            }
        });
    }

    public void close() {
        try {
            this.pool.close();
        }
        catch (InterruptedException e) {
        }
        try {
            if (this.conn != null) {
                this.conn.close();
                this.conn = null;
            }
        }
        catch (Exception x) {}
    }
    
    public long getRowsCount() {
        return this.nRows;
    }

    public Object getThroughput() {
        long elapse = end - start;
        long throughput = elapse == 0 ? 0 : this.nRows * 1000 / elapse;
        return throughput;
    }
    
    public void setThreads(int value) {
        this.nThreads = value;
    }

    public Connection getConnection() {
        return this.conn;
    }
}
