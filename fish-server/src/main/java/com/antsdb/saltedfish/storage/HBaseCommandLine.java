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
    private Configuration conf;
    private Connection conn;
    
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
        if (this.conn == null) {
            this.conn = ConnectionFactory.createConnection(getConfiguration());
            
        }
        return this.conn;
    }
    
    protected Configuration getConfiguration() {
        if (this.conf == null) {
            if (this.parser.hasOption("config")) {
                this.conf = HBaseConfiguration.create();
                this.conf = HBaseConfiguration.create();
                this.conf.addResource(new Path(this.parser.getOptionValue("config")));
            }
            else if (this.parser.hasOption("server")) {
                this.conf = HBaseConfiguration.create();
                this.conf.set("hbase.zookeeper.quorum", this.parser.getOptionValue("server"));
                this.conf.set("hbase.client.retries.number", "1");
                this.conf.set("zookeeper.recovery.retry", "1");
            }
            else {
                throw new RuntimeException("error: either --server or --config is not specified");
            }
        }
        return this.conf;
    }
}
