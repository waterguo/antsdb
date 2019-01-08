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
package com.antsdb.saltedfish.sql.mysql;

import java.io.File;

import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;

import com.antsdb.saltedfish.cpp.BetterCommandLine;

/**
 * 
 * @author *-xguo0<@
 */
public class MysqlParserMain extends BetterCommandLine {
    public static void main(String[] args) throws Exception {
        new MysqlParserMain().parseAndRun(args);
    }
    
    @Override
    protected void buildOptions(Options options) {
        options.addOption(null, "config", true, "directory of the hbase configuration");
    }

    @Override
    protected void run() throws Exception {
        if (this.cmdline.getArgs().length != 1) {
            println("error: missing arguments");
            System.exit(-1);
        }
        File file = new File(this.cmdline.getArgs()[0]);
        if (!file.exists()) {
            println("error: file not found");
            System.exit(-1);
        }
        String sql = FileUtils.readFileToString(file);
        println(sql);
        long start = System.nanoTime();
        int count = 10000;
        for (int i=0; i<count; i++) {
            parse(sql);
        }
        long end = System.nanoTime();
        println("%d ns", (end - start) / count);
    }

    private void parse(String sql) {
        MysqlParserFactory.parse(sql);
    } 

}
