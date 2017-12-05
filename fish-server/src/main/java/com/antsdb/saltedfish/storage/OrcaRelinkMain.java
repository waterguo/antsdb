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
import java.io.InputStreamReader;
import java.io.LineNumberReader;

import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;

import com.antsdb.saltedfish.nosql.ConfigService;
import com.antsdb.saltedfish.sql.FishCommandLine;

/**
 * 
 * @author *-xguo0<@
 */
public class OrcaRelinkMain extends FishCommandLine  {

    private ConfigService config;
    private String sysns;
    private Connection conn;

    public OrcaRelinkMain(String[] args) throws ParseException {
        super(args);
    }

    public static void main(String[] args) throws Exception {
        new OrcaRelinkMain(args).run();
    }

    @Override
    protected Options getOptions() {
        Options options = new Options();
        return options;
    }

    @Override
    protected String getName() {
        return null;
    }

    @Override
    protected String getCommandName() {
        return "antsdb-relink link the local AntsDB to an existing AntsDB instance in HBase";
    }

    private void run() throws Exception {
        File home = getHome();
        File configFile = new File(getHome(), "conf/conf.properties");
        if (!configFile.exists()) {
            println("error: config file is not found: %s", configFile);
            System.exit(-1);
        }
        
        // connecting

        println("AntsDB home: %s", home);
        this.config = new ConfigService(configFile);
        this.sysns = this.config.getSystemNamespace();
        Configuration hbaseconf = HBaseStorageService.getHBaseConfig(config);
        print("connecting to %s ... ", hbaseconf.get("hbase.zookeeper.quorum"));
        this.conn = HBaseStorageService.getConnection(hbaseconf);
        println("connected");
        
        // read checkpoint

        println("");
        TableName tn = TableName.valueOf(this.sysns, HBaseStorageService.TABLE_SYNC_PARAM);
        CheckPoint hbasecp = new CheckPoint(conn, tn, true);
        println("server id: %d", hbasecp.getServerId());
        println("log pointer: %x", hbasecp.getCurrentSp());
        
        // reset 
        
        print("Relink will erase all data stored at your local AntsDB folder. Enter [yes] to continue: ");
        String line = new LineNumberReader(new InputStreamReader(System.in)).readLine();
        if (!line.equalsIgnoreCase("yes")) {
            println("relink is canceled");
            return;
        }
        
        // delete files
        
        File cacheFolder = new File(home, "cache");
        if (cacheFolder.exists()) {
            print("deleting %s ... ", cacheFolder);
            FileUtils.cleanDirectory(cacheFolder);
            println("done");
        }
        File dataFolder = new File(home, "data");
        if (dataFolder.exists()) {
            print("deleting %s ... ", dataFolder);
            FileUtils.cleanDirectory(dataFolder);
            println("done");
        }
        
        // create checkpoint file

        print("creating new server id ... ");
        dataFolder.mkdirs();
        File cpFile = new File(dataFolder, "checkpoint.bin");
        com.antsdb.saltedfish.nosql.CheckPoint cp = new com.antsdb.saltedfish.nosql.CheckPoint(cpFile, true);
        cp.open();
        long newServerId = cp.getServerId();
        cp.close();
        println("done");
        println("new server id: %d", newServerId);
        
        // setting log pointer
        
        println("");
        print(
            "Confirm there is no active AntsDB instance linked to the HBase namespace %s/%s. " 
                + "Data will get corrupted with more than one AntsDB working on the same HBase namespace" 
                + ". Enter [yes] to continue: ",
            hbaseconf.get("hbase.zookeeper.quorum"),
            config.getSystemNamespace());
        line = new LineNumberReader(new InputStreamReader(System.in)).readLine();
        if (!line.equalsIgnoreCase("yes")) {
            println("relink is canceled");
            return;
        }
        print("setting hbase checkpoint ... ");
        hbasecp.setServerId(newServerId);
        hbasecp.setLogPointer(0);
        hbasecp.updateHBase();
        println("done");
        println("relink is completed");
    }

}
