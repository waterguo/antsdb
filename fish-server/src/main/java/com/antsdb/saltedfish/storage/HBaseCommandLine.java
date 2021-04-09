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

import java.io.File;
import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;
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
        options.addOption(null, "keytab", true, "kerberos keytab");
        options.addOption(null, "principal", true, "kerberos principal");
        options.addOption(null, "krb5conf", true, "kerberos configuration file");
        this.parser = parse(options, args);
    }
    
    public Connection getConnection() throws IOException {
        if (this.conn == null) {
            Configuration conf = getConfiguration();
            this.conn = ConnectionFactory.createConnection(conf);
        }
        return this.conn;
    }
    
    public Configuration getConfiguration() throws IOException {
        if (this.conf == null) {
            if (this.parser.hasOption("config")) {
                this.conf = HBaseConfiguration.create();
                this.conf.addResource(new Path(this.parser.getOptionValue("config")));
            }
            else if (this.parser.hasOption("server")) {
                this.conf = HBaseConfiguration.create();
                this.conf.set("hbase.zookeeper.quorum", this.parser.getOptionValue("server"));
            }
            else if (new File("/etc/hbase/conf/hbase-site.xml").exists()) {
                this.conf = HBaseConfiguration.create();
                this.conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
            }
            else {
                this.conf = HBaseConfiguration.create();
                this.conf.set("hbase.zookeeper.quorum", "localhost");
            }
            if (this.parser.hasOption("krb5conf")) {
                String principal = this.parser.getOptionValue("principal");
                String keytab = this.parser.getOptionValue("keytab");
                System.setProperty("java.security.krb5.conf", this.parser.getOptionValue("krb5conf"));
                // System.setProperty("sun.security.krb5.debug", "true");
                conf.set("hadoop.security.authentication", "kerberos");
                UserGroupInformation.setConfiguration(conf);
                UserGroupInformation.loginUserFromKeytab(principal, keytab);
            }
            this.conf.set("hbase.client.retries.number", "2");
            this.conf.set("hbase.client.operation.timeout", "3000");
        }
        return this.conf;
    }
}
