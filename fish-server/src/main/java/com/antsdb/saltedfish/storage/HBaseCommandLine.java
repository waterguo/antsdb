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

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.antsdb.saltedfish.util.CommandLineHelper;

/**
 * 
 * @author *-xguo0<@
 */
public abstract class HBaseCommandLine extends CommandLineHelper {
    protected CommandLine parser;
    
    abstract protected Options getOptions();

    static {
        Logger.getLogger(org.apache.hadoop.util.NativeCodeLoader.class).setLevel(Level.ERROR);
    }
    
    public HBaseCommandLine(String[] args) throws ParseException {
        Options options = getOptions();
        options.addOption(null, "server", true, "hbase quorum name");
        options.addOption(null, "config", true, "hbase config file (hbase-site.xml)");
        this.parser = parse(options, args);
    }
    
    public Connection getConnection() throws IOException {
        Connection result = null;
        if (this.parser.hasOption("config")) {
            result = connectUseConfig(this.parser.getOptionValue("config"));
        }
        else if (this.parser.hasOption("server")) {
            result = connectUseServer(this.parser.getOptionValue("server"));
        }
        else {
            println("error: either --server or --config is not specified");
        }
        return result;
    }
    
    private Connection connectUseServer(String optionValue) throws IOException {
        String zkserver = this.parser.getOptionValue("server");
        if (zkserver == null) {
            println("error: --server is not specified");
            return null;
        }
        println("Connecting to server %s ...", zkserver);
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", zkserver);
        conf.set("hbase.client.retries.number", "1");
        conf.set("zookeeper.recovery.retry", "1");
        Connection conn = ConnectionFactory.createConnection(conf);
        println("hbase connected - " + zkserver + "\n");
        return conn;
    }
    
    private Connection connectUseConfig(String optionValue) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf = HBaseConfiguration.create();
        conf.addResource(new Path(optionValue));
        println("Connecting to server %s ...", conf.get("hbase.zookeeper.quorum"));
        Connection conn = ConnectionFactory.createConnection(conf);
        return conn;
    }

}
