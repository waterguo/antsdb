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

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.nosql.HColumnRow;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.SysMetaRow;
import com.antsdb.saltedfish.sql.vdm.BlobReference;
import com.antsdb.saltedfish.util.UberFormatter;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author *-xguo0<@
 */
class CudHandler {
    private static Logger _log = UberUtil.getThisLogger();

    List<ReplicatorColumnMeta> columns = new ArrayList<>();
    List<String> key = new ArrayList<>();
    PreparedStatement insert;
    int[] insertParams;
    PreparedStatement update;
    int[] updateParams;
    PreparedStatement delete;
    String deleteSql;
    List<ReplicatorColumnMeta> deleteParams;
    PreparedStatement replace;
    List<ReplicatorColumnMeta> replaceParams;
    boolean isBlobTable;
    private SysMetaRow table;
    Connection conn;
    private String replaceSql;
    private boolean isPendingBlob = false;
    
    public void prepare(Connection conn, SysMetaRow table, List<HColumnRow> hcolumns) throws SQLException {
        this.conn = conn;
        this.table = table;
        
        DatabaseMetaData meta = conn.getMetaData();
        this.isBlobTable = table.isBlobTable();
        if (this.isBlobTable) {
            return;
        }
        
        // fetch columns
        
        loadColumnMeta(meta, table, hcolumns);
        if (this.columns.size() == 0) {
            throw new IllegalArgumentException("table not found : " + table.getTableName());
        }
        
        // fetch primary key
        
        try (ResultSet rs = meta.getPrimaryKeys(table.getNamespace(), null, table.getTableName())) {
            while (rs.next()) {
                String columnName = rs.getString(4);
                this.key.add(columnName);
            }
        }
        
        // build replace
        
        String sql = String.format("REPLACE INTO `%s`.`%s` VALUES (%s)", 
                table.getNamespace(), 
                table.getTableName(),
                repeat(this.columns.size()));
        this.replaceSql= sql;
        this.replace = conn.prepareStatement(sql);
        this.replaceParams = this.columns;
        
        // build delete
        
        StringBuilder buf = new StringBuilder();
        if (this.key.size() != 0) {
            for (int i=0; i<this.key.size(); i++) {
                if (i > 0) {
                    buf.append(" AND ");
                }
                buf.append('`');
                buf.append(this.key.get(i));
                buf.append('`');
                buf.append("<=>?");
            }
            this.deleteParams = buildParameters(this.key);
        }
        else {
            for (int i=0; i<this.columns.size(); i++) {
                if (i > 0) {
                    buf.append(" AND ");
                }
                buf.append('`');
                buf.append(this.columns.get(i).columnName);
                buf.append('`');
                buf.append("<=>?");
            }
            this.deleteParams = this.columns;
        }
        sql = String.format(
                "DELETE FROM `%s`.`%s` WHERE %s", 
                table.getNamespace(), 
                table.getTableName(), 
                buf.toString());
        this.deleteSql = sql;
        this.delete = conn.prepareStatement(sql);
    }
    
    private void loadColumnMeta(DatabaseMetaData meta, SysMetaRow table, List<HColumnRow> hcolumns) 
    throws SQLException {
        this.columns.clear();
        try (ResultSet rs = meta.getColumns(table.getNamespace(), null, table.getTableName(), "%")) {
            while (rs.next()) {
                ReplicatorColumnMeta columnMeta  = new ReplicatorColumnMeta();
                columnMeta.dataType = rs.getInt(5);
                columnMeta.typeName = rs.getString(6);
                columnMeta.columnName = rs.getString(4);
                columnMeta.nullable = rs.getInt(11);
                columnMeta.defaultValue = rs.getString(13);
                for (HColumnRow j:hcolumns) {
                    if (columnMeta.columnName.equalsIgnoreCase(j.getColumnName())) {
                        columnMeta.hcolumnPos = j.getColumnPos();
                    }
                }
                if (columnMeta.hcolumnPos == 0) {
                    String name = String.format("%s.%s.%s", 
                                                table.getNamespace(), 
                                                table.getTableName(), 
                                                columnMeta.columnName);
                    throw new IllegalArgumentException("column " + name + " not found");
                }
                this.columns.add(columnMeta);
            }
        }
    }

    private List<ReplicatorColumnMeta> buildParameters(List<String> columnNames) {
        List<ReplicatorColumnMeta> result = new ArrayList<>(columnNames.size());
        for (int i=0; i<columnNames.size(); i++) {
            String ii = columnNames.get(i); 
            for (ReplicatorColumnMeta j:this.columns) {
                if (ii.equals(j.columnName)) {
                    result.add(j); 
                }
            }
        }
        return result;
    }

    private String repeat(int n) {
        StringBuilder buf = new StringBuilder();
        for (int i=0; i<n; i++) {
            buf.append("?,");
        }
        buf.deleteCharAt(buf.length()-1);
        return buf.toString();
    }

    public void insert(long trxid, Row row, boolean isBlobRow, long lp, int tableId) throws SQLException {
        if (!this.isPendingBlob && isBlobRow) {
            // if incoming row is blob there is no pending updates, just return
            return;
        }
        boolean hasBlobRef = false;
        for (int i=0; i<this.replaceParams.size(); i++) {
            Object value = getValue(row, replaceParams.get(i), isBlobRow);
            if (isBlobRow) {
                if (value != null) {
                    this.replace.setObject(i+1, value);
                }
            }
            else {
                if (value instanceof BlobReference) {
                    hasBlobRef = true;
                    this.replace.setObject(i+1, null);
                }
                else {
                    this.replace.setObject(i+1, value);
                }
            }
        }
        if (hasBlobRef) {
            // if current row has blob reference, wait for the blob row to come
            this.isPendingBlob = true;
            return;
        }
        this.isPendingBlob = false;
        int result = this.replace.executeUpdate();
        if (_log.isTraceEnabled()) {
            String key = KeyBytes.toString(row.getKeyAddress());
            _log.trace("replace lp={} tableId={} key={} result={}", lp, tableId, key, result);
        }
        if (result != 1 && result != 2) {
            // 1 means an INSERT. 2 means an UPDATE
            log(result, this.replaceSql, replaceParams, row);
        }
    }

    public void update(long trxid, Row row, boolean isBlobRow, long lp, int tableId) throws SQLException {
        insert(trxid, row, isBlobRow, lp, tableId);
    }
    
    public void delete(Row row, long lp, int tableId) throws SQLException {
        for (int i=0; i<this.deleteParams.size(); i++) {
            Object value = getValue(row, this.deleteParams.get(i), false);
            this.delete.setObject(i+1, value);
        }
        int result = this.delete.executeUpdate();
        if (_log.isTraceEnabled()) {
            String key = KeyBytes.toString(row.getKeyAddress());
            _log.trace("delete lp={} tableId={} key={} result={}", lp, tableId, key, result);
        }
        if (result != 1) {
            log(result, this.deleteSql, this.deleteParams, row);
        }
    }
    
    private void log(int result, String sql, List<ReplicatorColumnMeta> params, Row row) {
        if (JdbcReplicator._jdbclog != null) {
            Object[] args = new Object[params.size()];
            for (int i=0; i<params.size(); i++) {
                Object value = getValue(row, params.get(i), false);
                args[i] = value;
            }
            try {
                JdbcReplicator._jdbclog.write(result, sql, args);
            }
            catch (IOException x) {
                _log.warn("unable to log to jdbc log", x);
            }
        }
    }

    @SuppressWarnings("unused")
    private void debugDelete(Row row) throws SQLException {
        for (int i=this.deleteParams.size(); i>0; i--) {
            StringBuilder sql = new StringBuilder();
            sql.append("SELECT * FROM `");
            sql.append(this.table.getNamespace());
            sql.append("`.`");
            sql.append(this.table.getTableName());
            sql.append("` WHERE ");
            for (int j=0; j<i; j++) {
                sql.append(this.deleteParams.get(j).columnName);
                sql.append("<=>?");
                if (j < i-1) {
                    sql.append(" AND ");
                }
            }
            try (PreparedStatement stmt=this.conn.prepareStatement(sql.toString())) {
                for (int j=0; j<i; j++) {
                    Object value = getValue(row, this.deleteParams.get(j), false);
                    stmt.setObject(j+1, value);
                }
                try (ResultSet rs=stmt.executeQuery()) {
                    if (rs.next()) {
                        ReplicatorColumnMeta column = this.deleteParams.get(i);
                        _log.debug("found record with sql: {}", sql.toString());
                        _log.debug("record not found with sql: {}", this.deleteSql);
                        _log.debug("last column: {}", column.columnName);
                        _log.debug("value in antsdb: {}", row.get(column.hcolumnPos));
                        _log.debug("value in mysql: {}", rs.getObject(column.columnName));
                        return;
                    }
                }
            }
        }
    }

    private Object getValue(Row row, ReplicatorColumnMeta meta, boolean isBlobRow) {
        Object result = row.get(meta.hcolumnPos);
        if ((result == null) && isBlobRow) {
            return null;
        }
        // special logic to handle mysql 0000-00-00 00:00
        if (result==null && meta.dataType==Types.TIMESTAMP && meta.nullable==DatabaseMetaData.columnNoNulls) {
            if ("0000-00-00 00:00:00".equals(meta.defaultValue)) {
                result = "0000-00-00 00:00:00";
            }
        }
        if (result==null && meta.dataType==Types.DATE && meta.nullable==DatabaseMetaData.columnNoNulls) {
            if ("0000-00-00".equals(meta.defaultValue)) {
                result = "0000-00-00";
            }
        }
        if (result instanceof Date && ((Date)result).getTime() == Long.MIN_VALUE) {
            result = "0000-00-00";
        }
        if (result instanceof Timestamp && ((Timestamp)result).getTime() == Long.MIN_VALUE) {
            result = "0000-00-00 00:00:00";
        }
        if (result instanceof Duration) {
            result = UberFormatter.duration((Duration)result);
        }
        return result;
    }

    boolean isBlobTable() {
        return this.isBlobTable;
    }
    
    @Override
    public String toString() {
        return this.table.toString();
    }
}
