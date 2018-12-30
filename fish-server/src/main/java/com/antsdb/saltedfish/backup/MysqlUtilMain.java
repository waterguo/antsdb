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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

import org.apache.commons.cli.Options;

import com.antsdb.saltedfish.cpp.BetterCommandLine;
import com.antsdb.saltedfish.slave.DbUtils;
import com.antsdb.saltedfish.util.MysqlJdbcUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class MysqlUtilMain extends BetterCommandLine {

    private String host;
    private String port;
    private String user;
    private String password;

    public static void main(String[] args) throws Exception {
        MysqlJdbcUtil.loadClass();
        new MysqlUtilMain().parseAndRun(args);
    }

    @Override
    protected void buildOptions(Options options) {
        options.addOption(null, "host", true, "host");
        options.addOption(null, "port", true, "port");
        options.addOption(null, "user", true, "user");
        options.addOption(null, "password", true, "password");
        options.addOption(null, "empty-database", true, "remove all tables from the specified database");
    }

    @Override
    protected void run() throws Exception {
        this.host = this.cmdline.getOptionValue("host", "localhost");
        this.port = this.cmdline.getOptionValue("port", "3306");
        this.user = this.cmdline.getOptionValue("user", "");
        this.password = this.cmdline.getOptionValue("password", "");
        
        if (this.cmdline.hasOption("empty-database")) {
            empty(this.cmdline.getOptionValue("empty-database"));
        }
    }

    private void empty(String db) throws Exception {
        String url = MysqlJdbcUtil.getUrl(this.host, Integer.parseInt(this.port), db);
        try (Connection conn = DriverManager.getConnection(url, this.user, this.password)) {
            DbUtils.execute(conn, "SET FOREIGN_KEY_CHECKS=0");
            DbUtils.execute(conn, "SET UNIQUE_CHECKS=0");
            ResultSet rs = conn.createStatement().executeQuery("show tables");
            while (rs.next()) {
                String table = rs.getString(1);
                println("removing table %s ...", table);
                String sql = "DROP TABLE " + table;
                DbUtils.execute(conn, sql);
            }
        }
    }

}
