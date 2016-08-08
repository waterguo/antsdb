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
import java.util.Map;
import java.util.NavigableMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.slf4j.Logger;

import com.antsdb.saltedfish.nosql.ConfigService;
import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.server.SaltedFish;
import com.antsdb.saltedfish.sql.OrcaConstant;
import com.antsdb.saltedfish.util.BytesUtil;
import com.antsdb.saltedfish.util.CommandLineHelper;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author wgu0
 */
public class HBaseUtilMain implements CommandLineHelper {
    static Logger _log = UberUtil.getThisLogger();

	private Connection conn;
	private boolean noversion;

	public static void main(String[] args) throws Exception {
		new HBaseUtilMain().run(args);
	}

	private Options getOptions() {
		Options options = new Options();
		options.addOption("h", "help", false, "print help");
		options.addOption(null, "clean", false, "delete all tables and namespaces");
		options.addOption(null, "list-ns", false, "list name spaces");
		options.addOption(null, "list-table", false, "list tables");
		options.addOption(null, "link", false, "reset hbase so that it can be relinked to an new antsdb instance");
		options.addOption(null, "dump", false, "dump table");
		options.addOption(null, "noversion", false, "dump the latest value of the cell instead of all versions");
		options.addOption(null, "server", true, "hbase zookeeper server(default is localhost)");
		options.addOption(null, "config", true, "hbase config file (hbase-site.xml)");
		options.addOption(null, "sync", false, "sync antsdb data to hbase");
		options.addOption(null, "home", true, "antsdb home directory");
		options.addOption(null, "skip", true, "skip table list - ex: xcar_data:car_dealer_models,xcar_data:version");
		options.addOption(null, "tables", true, "sync table list - ex: xcar_data:car_dealer_models,xcar_data:version");
		options.addOption(null, "ignore-error", false, "ingore error - trying to finish all import tables");
		options.addOption(null, "get-sp", false, "read current SP");
		options.addOption(null, "update-sp", true, "get/set current SP. 'r' to get. 'db' to set to antsdb value.");
		options.addOption(null, "update-sp-db", false, "set current SP to antsdb value.");
		options.addOption(null, "compare", true, "compare number of rows between antsdb and hbase (0 to compare all)");
		int cores = Runtime.getRuntime().availableProcessors();
		options.addOption(null, "threads", true, "thread count - default is cpu core count: " + cores);
		options.addOption(null, "checkrow", true, "show row value of specified table. input key in hex");
		return options;
	}

	private void run(String[] args) throws Exception {
		
		CommandLine line = parse(getOptions(), args);
		
		if (line.getOptions().length == 0 || line.hasOption('h')) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("hbase-util", getOptions());
			return;
		}

		// Connecting
		
		if (line.hasOption("config")) {
			connectUseConfig(line.getOptionValue("config"));
		}
		else  {
			String zkserver = "localhost";
			if (line.hasOption("server")) {
				zkserver = line.getOptionValue("server");
			}
			connect(zkserver);
		}
		
		// connect to hbase
		

		try {
			// options
			
			this.noversion = line.hasOption("noversion");
			
			// commands
			
			if (line.hasOption("clean")) {
				delete_all();
			}
			else if (line.hasOption("list-ns")) {
				list_ns();
			}
			else if (line.hasOption("list-table")) {
				list_table();
			}
			else if (line.hasOption("dump")) {
				if (line.getArgList().size() >= 1) {
					dump(line.getArgList().get(0));
				}
				else {
					println("error: table name is missing");
				}
			}
			else if (line.hasOption("link")) {
				link();
			}
			else if (line.hasOption("get-sp")) {
				// read current SP
				read_sp();
			}
			else if (line.hasOption("update-sp")) {
				// update current sp to specified value
				String updateSpValue = line.getOptionValue("update-sp", "");
				try {
					long currentSp = Long.parseLong(updateSpValue);
					update_sp(currentSp);
				}
				catch (Exception ex) {
					println("Invalid current SP value - " + updateSpValue + "\n");
				}
			}
			else if (line.hasOption("update-sp-db")) {
				// udpate current sp from antsdb
				File home = checkAntsDBHome(line);
				if (home != null) {			
					update_sp(home);
				}
			}
			else if (line.hasOption("sync")) {			
				File home = checkAntsDBHome(line);
				if (home != null) {					
					boolean ignoreError = line.hasOption("ignore-error");
					int cores = Runtime.getRuntime().availableProcessors();
					int threads = cores;
					if (line.hasOption("threads")) {
						threads = Integer.parseInt(line.getOptionValue("threads"));
						if (threads <= 0 || threads > 200) {
							threads = cores;
						}
					}
					
					String skipList = line.getOptionValue("skip", "").trim();
					String syncList = line.getOptionValue("tables", "").trim();				
					String[] skipTables = null;
					String[] syncTables = null;
					if (skipList != null && !skipList.isEmpty()) {
						skipTables = skipList.split(",");
					}
					if (syncList != null && !syncList.isEmpty()) {
						syncTables = syncList.split(",");
					}

					sync_antsdb(home, skipTables, syncTables, ignoreError, threads);
				}
			}
			else if (line.hasOption("compare")) {
				// udpate current sp from antsdb
				File home = checkAntsDBHome(line);
				if (home != null) {			
					boolean ignoreError = line.hasOption("ignore-error");
					String skipList = line.getOptionValue("skip", "").trim();
					String syncList = line.getOptionValue("tables", "").trim();				
					String[] skipTables = null;
					String[] syncTables = null;
					if (skipList != null && !skipList.isEmpty()) {
						skipTables = skipList.split(",");
					}
					if (syncList != null && !syncList.isEmpty()) {
						syncTables = syncList.split(",");
					}
					
					// update current sp to specified value
					int rows = 200;
					String s = line.getOptionValue("compare", "");
					try {
						rows = Integer.parseInt(s);
						if (rows < 0) throw new Exception();
					}
					catch (Exception ex) {
						println("Invalid compare rows count - " + s + "\n");
					}

					compare_hbase_rows(home, rows, skipTables, syncTables, ignoreError);
				}
			}
			else if (line.hasOption("checkrow")) {
				// udpate current sp from antsdb
				File home = checkAntsDBHome(line);
				if (home != null) {			
					String table = line.getOptionValue("tables", "").trim();
					String key = line.getOptionValue("checkrow", "").trim();
					if (table == null || table.isEmpty()) {
						println("No talbe specified.");
					}
					else if (key == null || key.isEmpty()) {
						println("No row key specified,");
					}
					else {
						checkRow(home, table, key);
					}
				}
			}
			else {
				println("error: command is missing");
			}
		}
		
		finally {
			
			// alway disconnect from hbase
			disconnect();
		}
	}

	private void connectUseConfig(String optionValue) throws IOException {
		Configuration conf = HBaseConfiguration.create();
        conf = HBaseConfiguration.create();
        conf.addResource(new Path(optionValue));
		println("Connecting to server %s ...", conf.get("hbase.zookeeper.quorum"));
		this.conn = ConnectionFactory.createConnection(conf);
	}

	private File checkAntsDBHome(CommandLine line) {
		if (line.hasOption("home")) {
			File home = new File(line.getOptionValue("home"));
			if (home.isDirectory()) {
				return home;
			}
			else {
				println("error: invalid home directory");
			}
		}
		else {
			println("error: antsdb home directory is missing");
		}
		return null;
	}
	
	private void link() throws IOException {
		Table table = this.conn.getTable(TableName.valueOf(OrcaConstant.SYSNS, CheckPoint.TABLE_SYNC_PARAM));
		Delete delete = new Delete(CheckPoint.KEY);
		table.delete(delete);
	}

	private void connect(String zkserver) throws IOException {
		println("Connecting to server %s ...", zkserver);
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", zkserver);
		this.conn = ConnectionFactory.createConnection(conf);
		println("hbase connected - " + zkserver + "\n");
	}
	
	private void disconnect() throws IOException {
		if (this.conn != null) {
			this.conn.close();
			println("hbase disconnected.");
		}
	}

	private void delete_all() throws IOException {
		Admin admin = conn.getAdmin();
		for (HTableDescriptor i:admin.listTables()) {
			TableName name = i.getTableName();
			println("deleting table %s ...", name.getNameAsString());
			try {
				admin.disableTable(name);
			}
			catch (Exception ignored) {}
			admin.deleteTable(name);
		}
		for (NamespaceDescriptor i:admin.listNamespaceDescriptors()) {
			String name = i.getName();
			if ("default".equals(name) || "hbase".equals(name)) {
				continue;
			}
			println("deleting namespace %s ...", name);
			admin.deleteNamespace(name);
		}
	}

	private void list_ns() throws IOException {
		Admin admin = conn.getAdmin();
		for (NamespaceDescriptor i:admin.listNamespaceDescriptors()) {
			println(i.getName());
		}
	}

	private void list_table() throws IOException {
		Admin admin = conn.getAdmin();
		for (HTableDescriptor i:admin.listTables()) {
			println(i.getNameAsString());
		}
	}

	private void dump(String name) throws IOException {
		Table htable = this.conn.getTable(TableName.valueOf(name));
		Scan scan = new Scan();
		ResultScanner rs = htable.getScanner(scan);
		for (Result r=rs.next(); r != null; r=rs.next()) {
			byte[] key = r.getRow();
			println(BytesUtil.toHex8(key));
			byte[] types = getTypes(r);
			if (this.noversion) {
				for (Map.Entry<byte[],NavigableMap<byte[],byte[]>> i:r.getNoVersionMap().entrySet()) {
					String cf =  new String(i.getKey());
					int idx = 0;
					for (Map.Entry<byte[],byte[]> j:i.getValue().entrySet()) {
						String q = Helper.getKeyName(j.getKey());
						String value = getCellText(types, idx, j.getValue());
						println("  %s:%s %s", cf, q, value);
					}
				}
			}
			else {
				for (Map.Entry<byte[],NavigableMap<byte[],NavigableMap<Long,byte[]>>> i:r.getMap().entrySet()) {
					String cf =  new String(i.getKey());
					int idx = 0;
					for (Map.Entry<byte[],NavigableMap<Long,byte[]>> j:i.getValue().entrySet()) {
						String q = Helper.getKeyName(j.getKey());
						for (Map.Entry<Long,byte[]> k:j.getValue().entrySet()) {
							Long version = k.getKey();
							String value = getCellText(types, idx, k.getValue());
							println("  %s:%s`%d %s", cf, q, version, value);
						}
						idx++;
					}
				}
			}
		}
	}
	
	private String getCellText(byte[] types, int idx, byte[] bytes) {
		return BytesUtil.toCompactHex(bytes);
		/* we cannot figure out the cell type because cell are ordered alphabetically by name. but type is ordered
		 * by column id 
		int type = types[idx];
		if (type == Value.FORMAT_UTF8) {
			return new String(bytes, Charsets.UTF_8);
		}
		else {
			return BytesUtil.toCompactHex(bytes);
		}
		*/
	}

	private byte[] getTypes(Result r) {
		NavigableMap<byte[], byte[]> sysFamily = r.getNoVersionMap().get(Helper.SYS_COLUMN_FAMILY_BYTES);
		if (sysFamily == null) {
			return null;
		}
		byte[] types = sysFamily.get(Helper.SYS_COLUMN_DATATYPE_BYTES);
		return types;
	}

	Algorithm getCompressionType(String compressor) {
		Algorithm compressionType = Algorithm.GZ;
		if (compressor.equalsIgnoreCase("GZ")) {
			compressionType = Algorithm.GZ;
		}
		else if (compressor.equalsIgnoreCase("snappy"))
			compressionType = Algorithm.SNAPPY;
		else if (compressor.equalsIgnoreCase("LZ4")) {
			compressionType = Algorithm.LZ4;
		}
		else if (compressor.equalsIgnoreCase("LZO")) {
			compressionType = Algorithm.LZO;
		}
		else if (compressor.equalsIgnoreCase("NONE")) {
			compressionType = Algorithm.NONE;
		}
		
		return compressionType;
	}

    void sync_antsdb(File home, String[] skipTables, String[] syncTables, 
    				boolean ignoreError, int threads) throws Exception {
        // load the test instance
        
        SaltedFish fish = new SaltedFish(home);
        try {
        	// start fish without hbase service, without netty
        	fish.startOrcaOnly();
        	
        	// conf.properties option used by hbase service
        	ConfigService config = fish.getOrca().getHumpback().getConfig();
            int columnsPerPut = config.getHBaseMaxColumnsPerPut();        
            String compressCodec = config.getHBaseCompressionCodec();
        	Algorithm compressionType = Algorithm.valueOf(compressCodec.toUpperCase());

        	HBaseUtilImporter importer = new HBaseUtilImporter(threads, fish.getOrca(), this.conn, 
        									skipTables, syncTables, ignoreError);
        	
        	// set compression type
        	importer.setCompressionType(compressionType);

        	// set maximum columns per put
        	importer.setColumnsPerPut(columnsPerPut);
        	
        	importer.run();
        	
        	String result = importer.getResult();
        	println(result + "\n");
        }
        catch (Exception ex) {
        	_log.error("hbase import failed - ", ex);
        }
        finally {
        	fish.shutdown();
        }
    }
    
    long read_sp() throws Exception {
    	long oldCurrentSp = -1;
        try {
        	oldCurrentSp = CheckPoint.readCurrentSPFromHBase(this.conn);        	
        	println("Current SP: " + oldCurrentSp + "\n");
        }
        catch (Exception ex) {
        	println("Failed to get hbase currentSP.");
        	ex.printStackTrace();
        }
        return oldCurrentSp;
    }
    
    void update_sp(long newCurrentSp) throws Exception {
        try {
        	long oldCurrentSp = CheckPoint.readCurrentSPFromHBase(this.conn);
        	
        	println("Old Current SP: " + oldCurrentSp);
        	if (newCurrentSp != oldCurrentSp) {
        		CheckPoint.udpateCurrentSPToHBase(this.conn, newCurrentSp);
            	long currentSp = CheckPoint.readCurrentSPFromHBase(this.conn);
            	println("New Current SP: " + currentSp + "\n");
        	}
    		else {
    			println("Current SP is same as antsdb. No need to change.\n");
    		}
        }
        catch (Exception ex) {
        	println("Faield to update hbase currentSP.");
        	ex.printStackTrace();
        }
    }

    
    void update_sp(File home) throws Exception {
        // load the test instance
        
        SaltedFish fish = new SaltedFish(home);
        
        try {
        	// start fish without hbase service, without netty
        	fish.startOrcaOnly();
        	
        	Humpback humpback = fish.getOrca().getHumpback();
        	CheckPoint cp = new CheckPoint(humpback, this.conn);
        	cp.readFromHBase(humpback);;
        	
        	println("*****************************************************");
        	println("Server Id:              " + cp.getServerId());
        	println("Old Current SP:         " + cp.getCurrentSp());
        	
    		// write current SP to __SYS.SYNCPARAM
    		long currentSp = humpback.getLatestSP();
        	println("Current SP from AntsDB: " + currentSp);
    		
    		if (currentSp != cp.getCurrentSp()) {
	    		cp.setCurrentSp(currentSp);
	    		
	        	CheckPoint.udpateCurrentSPToHBase(this.conn, currentSp);
            	currentSp = CheckPoint.readCurrentSPFromHBase(this.conn);
	    		println("\nCurrent SP set to:      " + currentSp);
    		}
    		else {
    			println("\nCurrent SP is same as antsdb. No need to change.");
    		}
    			
        	println("*****************************************************");
        }
        catch (Exception ex) {
        	println("Faield to update hbase currentSP");
        	ex.printStackTrace();
        }
        finally {
        	fish.shutdown();
        }
    }
    
    void compare_hbase_rows(File home, int testRows, String[] skipTables, String[] syncTables, 
						boolean ignoreError) throws Exception {
        // load the test instance
        
        SaltedFish fish = new SaltedFish(home);
        
        try {
        	// start fish without hbase service, without netty
        	fish.startOrcaOnly();
        	
        	HBaseUtilDataComparer importer = new HBaseUtilDataComparer(fish.getOrca(), this.conn, 
        			testRows, skipTables, syncTables, ignoreError);
			importer.run();
			
			String result = importer.getResult();
			println("\n\n" + result + "\n");
        	
        	// check each table
        }
        catch (Exception ex) {
        	println("Faield to check hbase rows.");
        	ex.printStackTrace();
        }
        finally {
        	fish.shutdown();
        }
    }
    
    void checkRow(File home, String table, String key) {
        // load the test instance
        
        SaltedFish fish = new SaltedFish(home);
        
        try {
        	// start fish without hbase service, without netty
        	fish.startOrcaOnly();
        	AntsDBValidator.checkTableRow(fish.getOrca(), table, key);
        }
        catch (Exception ex) {
        	println("Faield to check hbase rows.");
        	ex.printStackTrace();
        }
        finally {
        	fish.shutdown();
        }
    }
}
