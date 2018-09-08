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
package com.antsdb.saltedfish.nosql;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import com.antsdb.saltedfish.storage.HBaseStorageService;
import com.antsdb.saltedfish.util.CommandLineHelper;

/**
 * check the storage connection
 * 
 * @author *-xguo0<@
 */
public class HumpbackStorageCheckMain extends CommandLineHelper {

    private String[] args;
    private CommandLine parser;
    private File home;

    public HumpbackStorageCheckMain(String[] args) throws ParseException {
        this.args = args;;
    }

    public static void main(String[] args) throws Exception {
        new HumpbackStorageCheckMain(args).run();
    }

    @Override
    protected String getCommandName() {
        return "antsdb-check <home>";
    }

    protected Options getOptions() {
        Options options = new Options();
        return options;
    }

    private void run() throws Exception {
        this.parser = super.parse(getOptions(), this.args);
        if (this.parser.getArgList().size() != 1) {
            println("error: missing home directory");
            System.exit(0);
        }
        this.home = new File(this.parser.getArgList().get(0));
        
        // check home
        
        if (!this.home.isDirectory()) {
            println("error: home director %s doesn't exist", this.home);
            System.exit(-1);
        }
        
        // find configuration file
        
        File fileConf = new File(this.home, "conf/conf.properties");
        if (!fileConf.exists()) {
            println("error: configuration file %s doesn't exist", fileConf);
            System.exit(-1);
        }
        ConfigService conf = new ConfigService(fileConf);
        println("antsdb home: %s", this.home);
        println("antsdb configuration: %s", fileConf);
        
        // check engine
        
        String storage = conf.getStorageEngineName();
        println("storage: %s", storage);
        if (storage.equals("hbase")) {
            checkHbase(conf);
        }
    }

    private void checkHbase(ConfigService conf) throws Exception {
        // check configuration
        
        String hbaseConfPath = conf.getHBaseConf(); 
        if (hbaseConfPath != null) {
            if (!new File(hbaseConfPath).exists()) {
                println("error: hbase config %s is not found", hbaseConfPath);
                System.exit(0);
            }
            println("hbase config: %s", hbaseConfPath);
        }
        else if (conf.getProperty("hbase.zookeeper.quorum", null) == null) {
            println("error: hbase is not configured");
            System.exit(0);
        }
        
        // check the connection 
        
        Configuration hbaseConf = HBaseStorageService.getHBaseConfig(conf);
        hbaseConf.set("hbase.client.retries.number", "0");
        if (hbaseConf.get("hbase.zookeeper.quorum") != null) {
            println("zookeeper quorum: %s", hbaseConf.get("hbase.zookeeper.quorum"));
        }
        try {
            Connection conn = ConnectionFactory.createConnection(hbaseConf);
            conn.getAdmin().listNamespaceDescriptors();
            conn.close();
            println("quorum is connected");
        }
        catch (Exception x) {
            println("error: unable to connect to quorum");
        }
    }
}
