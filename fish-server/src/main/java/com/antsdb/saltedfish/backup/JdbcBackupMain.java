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

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.commons.cli.Options;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

import com.antsdb.saltedfish.cpp.BetterCommandLine;
import com.antsdb.saltedfish.slave.DbUtils;
import com.antsdb.saltedfish.util.IOUtils;
import com.antsdb.saltedfish.util.MysqlJdbcUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class JdbcBackupMain extends BetterCommandLine {
    static final int TYPE_NULL = 1;
    static final int TYPE_INTEGER = 2;
    static final int TYPE_LONG = 3;
    static final int TYPE_STRING = 4;
    static final int TYPE_BYTES = 5;
    static final int TYPE_TIMESTAMP = 6;
    static final int TYPE_DATE = 7;
    static final int TYPE_TIME = 8;
    static final int TYPE_DECIMAL = 9;
    static final int TYPE_FLOAT = 10;
    static final int TYPE_DOUBLE = 11;
    static final int TYPE_BIG_INTEGER = 12;
    static final int TYPE_BOOL = 13;
    @SuppressWarnings("deprecation")
    static final long ZERO_DATE_TIME = new Date(1-1900, 0, 1).getTime();
    
    private String host;
    private String port;
    private Connection conn;
    private String filter;
    private long nRows;
    private String user;
    private String password;
    private long limit;
    private String sql;
    private String name;
    
    public JdbcBackupMain() {
    }
    
    public JdbcBackupMain(String host, int port, String user, String password) {
        this.host = host;
        this.port = String.valueOf(port);
        this.user = user;
        this.password = password;
    }
    
    public static void main(String[] args) throws Exception {
        MysqlJdbcUtil.loadClass();
        new JdbcBackupMain().parseAndRun(args);
    }

    @Override
    protected void buildOptions(Options options) {
        options.addOption(null, "host", true, "host");
        options.addOption(null, "port", true, "port");
        options.addOption(null, "user", true, "user");
        options.addOption(null, "password", true, "password");
        options.addOption(null, "file",  true, "dump file");
        options.addOption(null, "limit", true, "limit number of rows");
        options.addOption(null, "filter", true, "table filter using java patterns");
        options.addOption(null, "sql", true, "backup the data from the specified query");
        options.addOption(null, "name", true, "name of query result");
        options.addOption(null, "db", true, "name of the database, optional");
    }

    @Override
    protected void run() throws Exception {
        this.host = this.cmdline.getOptionValue("host", "localhost");
        this.port = this.cmdline.getOptionValue("port", "3306");
        this.user = this.cmdline.getOptionValue("user", "");
        this.password = this.cmdline.getOptionValue("password", "");
        this.limit = Long.parseLong(this.cmdline.getOptionValue("limit", "-1"));
        this.filter = this.cmdline.getOptionValue("filter");
        if (this.cmdline.getArgs().length > 0) {
            this.filter = this.cmdline.getArgs()[0];
        }
        this.sql = this.cmdline.getOptionValue("sql");
        this.name = this.cmdline.getOptionValue("name");
        
        connect();
        
        if (this.sql != null) {
            backupQuery();
        }
        else {
            backupTable();
        }
    }

    private OutputStream createOutputStream() throws FileNotFoundException {
        OutputStream out;
        if (this.cmdline.hasOption("file")) {
            out = new FileOutputStream(this.cmdline.getOptionValue("file"));
        }
        else {
            out = System.out;
        }
        out = new BufferedOutputStream(out, 1024 * 1024);
        return out;
    }
    
    private void backupQuery() throws Exception {
        if (this.name == null) {
            println("error: -name option is mandator for query");
            System.exit(-1);
        }
        
        // save metadata
        long start = System.currentTimeMillis();
        Statement stmt = this.conn.createStatement();
        stmt.setFetchSize(Integer.MIN_VALUE);
        ResultSet rs = stmt.executeQuery(this.sql);
        ResultSetMetaData rsmeta = rs.getMetaData();
        OutputStream out = createOutputStream();
        BackupFile backup = new BackupFile();
        TableBackupInfo tbi = new TableBackupInfo();
        tbi.create = this.sql;
        String[] segments = StringUtils.split(this.name, '.');
        ArrayUtils.reverse(segments);
        tbi.table = segments[0];
        if (segments.length >= 2) tbi.catalog = segments[1];
        tbi.columns = new ColumnBackupInfo[rsmeta.getColumnCount()];
        for (int i=1; i<=rsmeta.getColumnCount(); i++) {
            ColumnBackupInfo cbi = new ColumnBackupInfo();
            cbi.name = rsmeta.getColumnName(i);
            tbi.columns[i-1] = cbi;
        }
        backup.addTable(tbi);
        backup.save(out);
        
        // save query result
        DataOutputStream dout = new DataOutputStream(out);
        eprintln("saving " + tbi.getFullName() + " ... ");
        dout.writeUTF(tbi.getFullName());
        this.nRows = writeResultSet(dout, rs);
        out.close();
        rs.close();
        stmt.close();
        long end = System.currentTimeMillis();
        report(backup, end - start);
    }

    private void backupTable() throws Exception {
        BackupFile backup = getBackupInfo();

        // save metadata
        OutputStream out = createOutputStream();
        backup.save(out);
        
        // save table dump
        long start = System.currentTimeMillis();
        writeDump(out, backup);
        out.close();
        long end = System.currentTimeMillis();
        
        report(backup, end - start);
    }

    private void report(BackupFile backup, long elapse) {
        long throughput = this.nRows == 0 ? 0 : this.nRows  * 1000 / elapse; 
        eprintln("total tables: %d", backup.tables.size());
        eprintln("total rows: %d", this.nRows);
        eprintln("throughput: %d rows/s", throughput);
    }
    
    private long writeResultSet(DataOutputStream dout, ResultSet rs) throws Exception {
        long count = 0;
        ResultSetMetaData rsmeta = rs.getMetaData();
        while (rs.next()) {
            writeRow(dout, rsmeta, rs);
            count++;
        }
        dout.writeInt(0);
        return count;
    }
    
    private void writeDump(OutputStream out, BackupFile backup) throws Exception {
        DataOutputStream dout = new DataOutputStream(out);
        for (TableBackupInfo table:backup.tables) {
            System.err.print("saving " + table.getFullName() + " ... ");
            dout.writeUTF(table.getFullName());
            String sql = String.format("SELECT SQL_NO_CACHE * FROM `%s`.`%s`", table.catalog, table.table);
            if (this.limit != -1) {
                sql += " LIMIT " + this.limit;
            }
            long count = 0;
            Statement stmt = this.conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setFetchSize(Integer.MIN_VALUE);
            try (ResultSet rs = stmt.executeQuery(sql)) {
                count += writeResultSet(dout, rs);
            }
            this.nRows += count;
            System.err.println(count + " rows");
        }
        dout.flush();
    }

    private void writeRow(DataOutputStream dout, ResultSetMetaData rsmeta, ResultSet rs) 
    throws SQLException, IOException {
        for (int i=1; i<=rsmeta.getColumnCount(); i++) {
            Object value = rs.getObject(i); 
            writeValue(dout, value);
        }
    }

    private void writeValue(DataOutputStream dout, Object value) throws IOException {
        if (value == null) {
            dout.writeByte(TYPE_NULL);
        }
        else if (value instanceof Integer) {
            dout.writeByte(TYPE_INTEGER);
            dout.writeInt((Integer)value);
        }
        else if (value instanceof Long) {
            dout.writeByte(TYPE_LONG);
            dout.writeLong((Long)value);
        }
        else if (value instanceof BigInteger) {
            dout.writeByte(TYPE_BIG_INTEGER);
            dout.writeUTF(((BigInteger)value).toString());
        }
        else if (value instanceof Float) {
            dout.writeByte(TYPE_FLOAT);
            dout.writeFloat((Float)value);
        }
        else if (value instanceof Double) {
            dout.writeByte(TYPE_DOUBLE);
            dout.writeDouble((Double)value);
        }
        else if (value instanceof BigDecimal) {
            dout.writeByte(TYPE_DECIMAL);
            dout.writeUTF(((BigDecimal)value).toString());
        }
        else if (value instanceof String) {
            dout.writeByte(TYPE_STRING);
            IOUtils.writeUtf(dout, (String)value);
        }
        else if (value instanceof byte[]) {
            dout.writeByte(TYPE_BYTES);
            byte[] bytes = (byte[])value;
            IOUtils.writeCompactLong(dout, bytes.length);
            dout.write(bytes);
        }
        else if (value instanceof Timestamp) {
            Timestamp temp = (Timestamp)value;
            if (isZeroDateTime(temp)) {
                writeValue(dout, "0000-00-00 00:00:00");
            }
            else {
                dout.writeByte(TYPE_TIMESTAMP);
                dout.writeLong(temp.getTime());
            }
        }
        else if (value instanceof Date) {
            Date temp = (Date)value;
            if (isZeroDateTime(temp)) {
                writeValue(dout, "0000-00-00");
            }
            else {
                dout.writeByte(TYPE_DATE);
                dout.writeLong(temp.getTime());
            }
        }
        else if (value instanceof Time) {
            dout.writeByte(TYPE_TIME);
            Time temp = (Time)value;
            dout.writeLong(temp.getTime());
        }
        else if (value instanceof Boolean) {
            dout.writeByte(TYPE_BOOL);
            dout.writeBoolean((Boolean)value);
        }
        else {
            throw new IllegalArgumentException(value.getClass().toString());
        }
    }

    private boolean isZeroDateTime(java.util.Date value) {
        long tick = value.getTime();
        return tick == ZERO_DATE_TIME;
    }

    public BackupFile getBackupInfo() throws SQLException {
        BackupFile result = new BackupFile();
        List<String> databases = getDatabases();
        for (String i:databases) {
            if (i.equalsIgnoreCase("antsdb")) {
                continue;
            }
            result.tables.addAll(getTables(i));
        }
        return result;
    }

    private List<String> getDatabases() throws SQLException {
        List<String> result = new ArrayList<>();
        String url = MysqlJdbcUtil.getUrl(this.host, Integer.parseInt(this.port), null);
        Properties props = new Properties();
        props.setProperty("user", this.user==null ? "" : this.user);
        props.setProperty("password", this.password==null ? "" : this.password);
        props.setProperty("useServerPrepStmts", "true");
        try (Connection conn = DriverManager.getConnection(url,props)) {
            ResultSet rs = conn.createStatement().executeQuery("show databases");
            while (rs.next()) {
                result.add(rs.getString(1));
            }
        }
        return result;
    }
    
    private List<TableBackupInfo> getTables(String db) throws SQLException {
        Pattern ptn = null;
        if (this.filter != null) {
            ptn = Pattern.compile(this.filter.replace(".","\\.").replace("*",".*"));
        }
        List<TableBackupInfo> result = new ArrayList<>();
        try (ResultSet rs = conn.getMetaData().getTables(db, null, "%", new String[] {"TABLE"})) {
            while (rs.next()) {
                String catalog = rs.getString(1);
                String schema = rs.getString(2);
                String table = rs.getString(3);
                TableBackupInfo info = new TableBackupInfo();
                info.catalog = catalog;
                info.schema = schema;
                info.table = table;
                info.create = getCreateTable(conn, info);
                info.columns = getcolumns(conn, info);
                if (ptn != null) {
                    if (!ptn.matcher(info.getFullName()).matches()) {
                        continue;
                    }
                }
                result.add(info);
            }
        }
        return result;
    }

    private ColumnBackupInfo[] getcolumns(Connection conn, TableBackupInfo info) throws SQLException {
        List<ColumnBackupInfo> columns = new ArrayList<>();
        try (ResultSet rs = conn.getMetaData().getColumns(info.catalog, info.schema, info.table, "%")) {
            while (rs.next()) {
                ColumnBackupInfo column = new  ColumnBackupInfo();
                column.name = rs.getString(4);
                columns.add(column);
            }
        }
        return columns.toArray(new ColumnBackupInfo[columns.size()]);
    }

    private String getCreateTable(Connection conn, TableBackupInfo info) throws SQLException {
        String sql = String.format("SHOW CREATE TABLE `%s`.`%s`", info.catalog, info.table);
        String result = (String)DbUtils.firstRow(conn, sql).get("Create Table");
        return result;
    }

    public void connect() throws SQLException {
        String db = this.cmdline.getOptionValue("db");
        String url = MysqlJdbcUtil.getUrl(this.host, Integer.parseInt(this.port), db);
        Properties props = new Properties();
        props.setProperty("user", this.user);
        props.setProperty("password", this.password);
        props.setProperty("useServerPrepStmts", "true");
        props.setProperty("zeroDateTimeBehavior", "round");
        this.conn = DriverManager.getConnection(url, props);
    }
}
