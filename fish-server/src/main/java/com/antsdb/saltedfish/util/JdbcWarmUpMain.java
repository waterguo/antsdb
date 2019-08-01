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
package com.antsdb.saltedfish.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.cli.Options;

import com.antsdb.saltedfish.cpp.BetterCommandLine;

/**
 * warm up the cache by issue an SELECT COUNT(*) on specified tables
 * 
 * @author *-xguo0<@
 */
public class JdbcWarmUpMain extends BetterCommandLine {

    public static void main(String[] args) throws Exception {
        new JdbcWarmUpMain().parseAndRun(args);
    }
    
    @Override
    protected void buildOptions(Options options) {
        options.addOption("u", "user", true, "user name");
        options.addOption("p", "password", true, "password");
    }

    @Override
    protected String getCommandName() {
        return "warmup <url> [pattern]";
    }

    @Override
    protected void run() throws Exception {
        if (this.cmdline.getArgs().length < 1) {
            println("error: url is missing");
            return;
        }
        
        // get arguments
        String user = this.cmdline.getOptionValue("user");
        String password = this.cmdline.getOptionValue("password");
        String url = this.cmdline.getArgs()[0];
        //String pattern = (this.cmdline.getArgs().length >= 2) ? this.cmdline.getArgs()[1] : null;
        
        // 
        Connection conn = createConnection(url, user, password);
        long start = System.currentTimeMillis();
        int tcount = 0;
        long rcount = 0;
        try (ResultSet rs = conn.getMetaData().getTables(null, null, "%", new String[] {"TABLE"})) {
            while(rs.next()) {
                String catalog = rs.getString(1);
                String table = rs.getString(3);
                String fullname = String.format("`%s`.`%s`", catalog, table);
                rcount += warmup(conn, fullname);
                tcount++;
            }
        }
        long duration = System.currentTimeMillis() - start;
        println("%d tables warmed up in %s, %d records in total", 
                tcount, 
                UberFormatter.time(duration), 
                rcount);
    }

    private long warmup(Connection conn, String fullname) throws SQLException {
        long start = System.currentTimeMillis();
        String sql = "SELECT COUNT(*) FROM " + fullname;
        print("warming up %s ... ", fullname);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        long result = 0;
        if (rs.next()) {
            result = rs.getLong(1);
        }
        long duration = System.currentTimeMillis() - start;
        println("%d records in %s", result, UberFormatter.time(duration));
        rs.close();
        stmt.close();
        return result;
    }

    private Connection createConnection(String url, String user, String password) throws Exception {
        Connection result;
        loadDriver(url);
        if (user != null) {
            result = DriverManager.getConnection(url, user, password);
        }
        else {
            result = DriverManager.getConnection(url);
        }
        return result;
    }

    private void loadDriver(String url) throws ClassNotFoundException {
        Class.forName("com.mysql.jdbc.Driver");
    }

}
