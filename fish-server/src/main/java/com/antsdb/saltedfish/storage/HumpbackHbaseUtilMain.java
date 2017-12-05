/*-------------------------------------------------------------------------------------------------
 _______ __   _ _______ _______ ______  ______
 |_____| | \  |    |    |______ |     \ |_____]
 |     | |  \_|    |    ______| |_____/ |_____]

 Copyright (c) 2016, antsdb.com and/or its affiliates. All rights reserved. *-xguo0<@

 This program is free software: you can redistribute it and/or modify it under the terms of the
 GNU Affero General Public License, version 3, as published by the Free Software Foundation.

 You should have received a copy of the GNU Affero General Public License along with this program.
 If not, see <https://www.gnu.org/licenses/agpl-3.0.txt>
-------------------------------------------------------------------------------------------------*/
package com.antsdb.saltedfish.storage;

import java.io.File;
import java.io.IOException;

import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;

import com.antsdb.saltedfish.nosql.ConfigService;
import com.antsdb.saltedfish.sql.FishCommandLine;

/**
 * 
 * @author *-xguo0<@
 */
public class HumpbackHbaseUtilMain extends FishCommandLine {

    private ConfigService config;
    private String sysns;
    private Connection conn;
    private TableName tn;

    public HumpbackHbaseUtilMain(String[] args) throws ParseException {
        super(args);
    }

    public static void main(String[] args) throws Exception {
        new HumpbackHbaseUtilMain(args).run();;
    }

    @Override
    protected Options getOptions() {
        Options options = new Options();
        options.addOption(null, "link", false, "link the current antsdb to the hbase defined in configuration");
        options.addOption(null, "info", false, "show the hbase checkpoint information");
        options.addOption(null, "setlp", true, "set the lp to the specified value");
        return options;
    }

    @Override
    protected String getName() {
        return null;
    }

    @Override
    protected String getCommandName() {
        return "antsdb-hbase";
    }

    void init() throws Exception {
        File configFile = new File(getHome(), "conf/conf.properties");
        if (!configFile.exists()) {
            println("error: config file is not found: %s", configFile);
            System.exit(-1);
        }
        this.config = new ConfigService(configFile);
        this.sysns = this.config.getSystemNamespace();
        Configuration hbaseconf = HBaseStorageService.getHBaseConfig(config);
        this.conn = HBaseStorageService.getConnection(hbaseconf);
    }
    
    void run() throws Exception {
        this.tn = TableName.valueOf(this.sysns, HBaseStorageService.TABLE_SYNC_PARAM);
        if (this.cmd.hasOption("info")) {
            init();
            info();
        }
        else if (this.cmd.hasOption("setlp")) {
            long value = parseLong(this.cmd.getOptionValue("setlp"));
            init();
            setlp(value);
        }
    }

    private void setlp(long value) throws IOException {
        CheckPoint cp = getCheckPoint(true);
        cp.setLogPointer(value);
        cp.updateHBase();
    }

    private void info() throws IOException {
        println(getCheckPoint(false).toString());
    }

    CheckPoint getCheckPoint(boolean mutable) throws IOException {
        CheckPoint cp = new CheckPoint(conn, this.tn, mutable);
        return cp;
    }
}
