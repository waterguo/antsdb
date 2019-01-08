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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 
 * @author *-xguo0<@
 */
public class DbUtils {
    public static Properties properties(Connection conn, String sql) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            try (ResultSet rs =stmt.executeQuery(sql)) {
                Properties result = new Properties();
                while(rs.next()) {
                    String key = rs.getString(1);
                    String value = rs.getString(2);
                    result.setProperty(key, value);
                }
                return result;
            }
        }
    }
    
    public static List<Map<String, Object>> rows(Connection conn, String sql) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            try (ResultSet rs =stmt.executeQuery(sql)) {
                List<Map<String, Object>> result = new ArrayList<>();
                while(rs.next()) {
                    Map<String, Object> row = toRow(rs);
                    result.add(row);
                }
                return result;
            }
        }
    }
    
    public static Map<String, Object> firstRow(Connection conn, String sql) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            try (ResultSet rs =stmt.executeQuery(sql)) {
                if (!rs.next()) {
                    return null;
                }
                Map<String, Object> result = toRow(rs);
                return result;
            }
        }
    }

    public static Map<String, Object> firstRow(Connection conn, String sql, Object... args) 
    throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            for (int i=0; i<args.length; i++) {
                stmt.setObject(i+1, args[i]);
            }
            try (ResultSet rs =stmt.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                Map<String, Object> result = toRow(rs);
                return result;
            }
        }
    }
    
    private static Map<String, Object> toRow(ResultSet rs) throws SQLException {
        Map<String, Object> result = new HashMap<>();
        ResultSetMetaData meta = rs.getMetaData();
        for (int i=1; i<=meta.getColumnCount(); i++) {
            Object value = rs.getObject(i);
            result.put(meta.getColumnName(i), value);
        }
        return result;
    }

    public static void execute(Connection conn, String sql) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    public static int executeUpdate(Connection conn, String sql, Object... args) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            for (int i=0; i<args.length; i++) {
                stmt.setObject(i+1, args[i]);
            }
            return stmt.executeUpdate();
        }
    }

    public static boolean ping(Connection conn) {
        try {
            execute(conn, "/* ping */ SELECT 1");
            return true;
        } 
        catch (Exception x) {
            return false;
        }
    }

    public static void closeQuietly(Connection conn) {
        try {
            conn.close();
        }
        catch (SQLException e) {
        }
    }
}
