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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

import org.apache.commons.cli.Options;

import com.antsdb.saltedfish.cpp.BetterCommandLine;

/**
 * 
 * @author *-xguo0<@
 */
public class JdbcPrepareRunMain extends BetterCommandLine {

    public static void main(String[] args) throws Exception {
        new JdbcPrepareRunMain().parseAndRun(args);
    }
    
    @Override
    protected void buildOptions(Options options) {
    }

    @Override
    protected String getCommandName() {
        return "jdbc-prepare-run <url> <sql> <parameters>*";
    }

    @Override
    protected void run() throws Exception {
        if (this.cmdline.getArgList().size() < 2) {
            println("error: invalid parameters");
            return;
        }
        Class.forName("com.mysql.jdbc.Driver");
        String url = this.cmdline.getArgs()[0];
        String sql = this.cmdline.getArgs()[1];
        Connection conn = DriverManager.getConnection(url);
        PreparedStatement stmt = conn.prepareStatement(sql);
        for (int i=2; i<this.cmdline.getArgs().length; i++) {
            String value = this.cmdline.getArgs()[i];
            stmt.setString(i-1, value);
        }
        boolean result = stmt.execute();
        if (result) {
            ResultSet rs = stmt.getResultSet();
            ResultSetMetaData meta = rs.getMetaData();
            while (rs.next()) {
                for (int i=1; i<=meta.getColumnCount(); i++) {
                    print("%s", rs.getString(i));
                }
                println("");
            }
        }
        else {
            println("%d", stmt.getUpdateCount());
        }
    }

}
