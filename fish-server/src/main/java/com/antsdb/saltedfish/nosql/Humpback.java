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
package com.antsdb.saltedfish.nosql;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.MemoryManager;
import com.antsdb.saltedfish.cpp.Value;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.vdm.KeyMaker;
import com.antsdb.saltedfish.storage.HBaseStorageService;
import com.antsdb.saltedfish.storage.Helper;
import com.antsdb.saltedfish.util.CodingError;
import com.antsdb.saltedfish.util.FishJobManager;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * transaction id:
 * 
 * transaction time stamp:
 * 
 * <h2>Case Sensitivity</h2> 
 * 
 * Humpback is a partial case sensitive system. It follows the following rules
 * <ul>
 * <li>Table/Namespace lookup is case insensitive</li>
 * <li>Table/Namespace is preserved in case sensitive way as specified from input</li>
 * <li>Name conflict is detected in a case insensitive way, meaning Customer table and customer table cannot both
 * exist in the system</li>
 * </ul>
 * 
 * @author *-xguo0<@
 *
 */
public final class Humpback {
    public final static Character NULL = Character.MIN_VALUE;
    
	final static int DEFAULT_FILE_SIZE = 64 * 1024 * 1024;
    
    static List<Humpback> _instances = Collections.synchronizedList(new ArrayList<>());
    static Logger _log = UberUtil.getThisLogger();
    
    File home;
    File data;
    ConcurrentMap<Integer, GTable> tableById = new ConcurrentHashMap<>();
    ConcurrentMap<String, String> namespaces = new ConcurrentHashMap<String, String>();
    boolean isClosed = false;
    TrxMan trxMan = new TrxMan();
    CheckPoint cp;
    GTable sysmeta;
    Gobbler gobbler;
    SpaceManager spaceman;
	HBaseStorageService hbaseService = null;
	FishJobManager jobman = new FishJobManager();
	
	private ScheduledFuture<?> carbonizer;
	private ScheduledFuture<?> compactor;
	private ShutdownThread hook;
	private ConfigService config;
	private RowLockMonitor rowLockMonitor = new RowLockMonitor();
	private volatile long lastClosedTrxId;

	private boolean forceDisableHBase;

    class ShutdownThread extends Thread {
        @Override
        public void run() {
            shutdown();
        }
    }

    private Humpback(File dbfolder) throws Exception {
    	this(dbfolder, false);
    }
    
    private Humpback(File dbfolder, boolean forceDisableHBase) {
    	this.home = dbfolder;
        this.data = new File(dbfolder, "data");
        this.forceDisableHBase = forceDisableHBase;
    }
    
    public void open() throws Exception {
        File conf = new File(this.home, "conf"); 
        if (conf.exists()) {
            // new style, conf folder under home
            this.config = new ConfigService(new File(conf, "conf.properties"));
        }
        else {
            // old style
            this.config = new ConfigService(new File(this.home, "conf.properties"));
        }
        if (forceDisableHBase) {
        	this.config.props.remove("hbase_conf");
        }

        // create database folder if absent
        
        _log.info("Humpback home: " + this.home.getAbsolutePath());
        if (!data.isDirectory()) {
            _log.info("data directory is not found, creating one");
            data.mkdirs();
        }
        
        // initialize 
        
        this.cp = new CheckPoint(new File(this.data, "checkpoint.bin"));
        boolean isRecovering = this.cp.isDatabaseOpen();
        init(isRecovering);

        // add shutdown hook
        
        this.hook = new ShutdownThread();
        Runtime.getRuntime().addShutdownHook(this.hook);
        _instances.add(this);
    }
    
    public static Humpback open(File home) throws Exception {
    	return open(home, false);
    }
    
    public static Humpback open(File home, boolean forceDisableHBase) throws Exception {
    	Humpback humpback = new Humpback(home, forceDisableHBase);
    	boolean success = false;
    	try {
    		humpback.open();
    		success = true;
    		return humpback;
    	}
    	finally {
    		if (!success) {
    			humpback.shutdown();
    		}
    	}
    }
    
    public void setHBaseStorageService(HBaseStorageService hbaseService) {
    	this.hbaseService = hbaseService;
    }

    void init(boolean isRecovering) throws Exception {
        if (!this.data.isDirectory()) {
        	throw new HumpbackException("home director is not found");
        }

        // init space
        
        this.spaceman = new SpaceManager(this.data, config.getSpaceFileSize());
        this.spaceman.init();
        
        // init logger
        
        this.gobbler = new Gobbler(this.spaceman, config.isLogWriterEnabled());
        
        // hbase
        
    	File hbaseConfFile = this.config.getHBaseConfFile();
    	if (hbaseConfFile != null) {
        	this.hbaseService = new HBaseStorageService(this.config, this);
    	}
        
    	// initialize sysmeta table
    	
    	initSysmeta(isRecovering);
    	
        // load data
        
        loadData(isRecovering);
        
        // data recovery

        recover();
        
        // marking database is open
        
        this.cp.setDatabaseOpen(true);
        
        // recover transactions for hbase replication
        
        recoverTrx();
        
        // validate
        
        if (!validate()) {
        	throw new OrcaException("failed in validation");
        }
        
        // background jobs
        
        this.jobman.schedule(10, TimeUnit.SECONDS, ()-> {
        	if (this.isClosed) {
        		return;
        	}
            this.carbonizer = this.jobman.scheduleWithFixedDelay(new Carbonizer(this), 2, TimeUnit.SECONDS);
            this.compactor = this.jobman.scheduleWithFixedDelay(30, TimeUnit.SECONDS, new Compactor(this));
        });
    }

	private void initSysmeta(boolean isRecovering) throws IOException {
        this.sysmeta = new GTable(this, "", Integer.MAX_VALUE, DEFAULT_FILE_SIZE);
        this.sysmeta.open(isRecovering);
        
        // sysmeta exists
        
        if (!this.sysmeta.memtable.isPureEmpty()) {
        	return;
        }
        
        // if not exists, try to restore from hbase. this is a ugly hack :(
        
        if (this.hbaseService == null) {
        	return;
        }
        if (!this.hbaseService.doesTableExists("__SYS", "SYSTABLE")) {
        	return;
        }
        try {
        	List<Map<String,byte[]>> rows = this.hbaseService.getAll("__SYS", "SYSTABLE");
        	_log.info("rebuilding sysmeta ...");
        	for (Map<String,byte[]> row:rows) {
        		int id = Bytes.toInt(row.get("d:id"));
        		String ns = (String)Helper.hBaseDataToObject(Value.FORMAT_UNICODE16, row.get("d:namespace"));
        		String name = (String)Helper.hBaseDataToObject(Value.FORMAT_UNICODE16, row.get("d:table_name"));
        		SysMetaRow metarow = new SysMetaRow(id);
        		metarow.setTableId(id);
        		metarow.setNamespace(ns);
        		metarow.setTableName(name);
        		this.sysmeta.put(1, metarow.row);
        	}
        }
        catch (Exception x) {
        	_log.info("antsdb metadata not found in hbase", x);
        }
	}

	private void loadData(boolean isRecovering) throws Exception {
        // load name spaces
        
        for (File i:this.data.listFiles()) {
        	if (i.isDirectory()) {
        		String ns = i.getName();
        		this.namespaces.put(ns.toLowerCase(), ns);
        	}
        }

    	// load tables
    	
        for (RowIterator i=this.sysmeta.scan(1, 1);;) {
        	if (!i.next()) {
        		break;
        	}
        	SysMetaRow row = new SysMetaRow(SlowRow.from(i.getRow()));
        	int id = row.getTableId();
        	String ns = row.getNamespace();
        	if (!this.namespaces.containsKey(ns.toLowerCase())) {
        		createNamespace(ns);
        	}
            GTable gtable = new GTable(this, ns, id, DEFAULT_FILE_SIZE);
            gtable.open(isRecovering);
            this.tableById.put(gtable.getId(), gtable);
        }
	}

    public synchronized void shutdown() {
        if (this.isClosed) {
            return;
        }
        this.isClosed = true;
        
        // close write ahead logger
        
        if (this.gobbler != null) {
            this.gobbler.close();
        }
        if (this.spaceman != null) {
        	this.spaceman.close();
        }
        
        // terminate backend jobs
        
        try {
        	if (this.carbonizer != null) {
	            this.carbonizer.cancel(false);
				this.carbonizer.get();
        	}
        	if (this.compactor != null) {
    			this.compactor.get();
        	}
        	if (this.jobman != null) {
        		this.jobman.close();
        	}
		} 
        catch (CancellationException ignored) {
        }
        catch (Exception e) {
        	_log.warn("errors shutting down carbonizer", e);
		}
        
        // close trxman
        
        this.trxMan.close();
        this.lastClosedTrxId = this.trxMan.getLastTrxId();
        
        // write all data files
        
        try {
            _log.info("shutting down humpback...");
            flush();
            _log.info("humpback stopped peacefully");
        }
        catch (Exception x) {
            _log.error("failed to shutdown", x);
        }
        
        // validate
        
        validate();
        
        // close all tables
        
        for (GTable i:this.tableById.values()) {
        	i.close();
        }
        if (this.sysmeta != null) {
        	this.sysmeta.close();
        }
        
        // mark database properly closed
        
        this.cp.setDatabaseOpen(false);
        try {
            this.cp.close();
        }
        catch (IOException x) {
            _log.error("unable to update checkpoint file", x);
        }
        
        // close job manager
        
        this.jobman.close();
        
        // memory report
        
        _instances.remove(this);
        if (_instances.size() == 0) {
        	MemoryManager.report();
        }
    }
    
	public void flush() throws Exception {
        _log.info("flushing...");
        
        // save system meta

        if (!this.sysmeta.memtable.carbonize()) {
        	throw new OrcaException("unable to carbonize sysmeta");
        }

        // save normal table
        
        for (Map.Entry<Integer, GTable> i:this.tableById.entrySet()) {
            GTable table = i.getValue();
            if (!table.memtable.carbonize()) {
            	throw new OrcaException("unable to carbonize " + table.toString());
            }
        }
    }
    
    public void createNamespace(String name) throws HumpbackException {
        String key = name.toLowerCase();
        if (this.namespaces.putIfAbsent(key, name) != null) {
            throw new HumpbackException("namespace folder already exists");
        }
        File nsfolder = new File(this.data, name);
        if (!nsfolder.mkdirs()) {
            this.namespaces.remove(key);
            throw new HumpbackException("failed to create namespace folder");
        }

        // Create namespace in HBase
        if (hbaseService != null) {
        	hbaseService.createNamespace(name);
        }
    }
    
    public void dropNamespace(String name) throws HumpbackException {
        String key = name.toLowerCase();
        String realName = this.namespaces.remove(key);
        if (realName == null) {
            throw new HumpbackException("namespace doesn't exist: " + name);
        }
        for (GTable i:getTables(name)) {
            dropTable(name, i.getId());
        }
        File nsfolder = new File(this.data, realName);
        for (File i:nsfolder.listFiles()) {
            i.delete();
        }
        nsfolder.delete();

        // remove namespace from HBase
        if (hbaseService != null) {
        	hbaseService.dropNamespace(realName);
        }
    }
    
    public String getNamespace(String ns) {
        return this.namespaces.get(ns.toLowerCase());
    }
    
    public List<String> getNamespaces() {
        return new ArrayList<String>(this.namespaces.values());
    }
    
    public synchronized GTable createTable(String ns, String name, int id, TableType type) {
        return createTable(ns, name, id, true, type);
    }
    
    private synchronized GTable createTable(String ns, String name, int id, boolean createMeta, TableType type) {
        // create table space
        
        ns = this.namespaces.get(ns.toLowerCase());
        if (ns == null) {
            throw new HumpbackException("namespace does not exist");
        }
        if (getTable(id) != null) {
            throw new HumpbackException("table id alread exists: " + id);
        }
        GTable gtable;
		try {
			gtable = new GTable(this, ns, id, DEFAULT_FILE_SIZE);
	        tableById.put(id, gtable);
	        
	        // update meta data
	        
	        if (createMeta) {
	        	SysMetaRow row = new SysMetaRow(id);
	        	row.setTableId(id);
	        	row.setNamespace(ns);
	        	row.setTableName(name);
	        	row.setType(type);
	            this.sysmeta.put(1, row.row);
	        }

	        // create table in HBase
	        if (hbaseService != null) {
	        	hbaseService.createTable(ns, name);
	        }

	        return gtable;
		}
		catch (Exception x) {
			throw new HumpbackException(x);
		}
    }
    
    public synchronized void dropTable(String ns, int id) throws HumpbackException {
        GTable table = this.tableById.get(id);
        SysMetaRow meta = getTableInfo(id);
        if (table == null || meta == null) {
        	_log.warn("table {} doesn't exist", id);
        	return;
        }

        String namespace = meta.getNamespace();
        String tableName = meta.getTableName();
        
        table.drop();
        this.tableById.remove(id);

        // update meta data
        
        this.sysmeta.delete(1, KeyMaker.make(id), 1000);

        // drop table in HBase
        if (hbaseService != null) {
        	hbaseService.dropTable(namespace, tableName);
        }
    }
    
    public synchronized void truncateTable(String ns, int id) throws HumpbackException {
    	if (hbaseService != null) {
    		
    		long spNow = spaceman.getAllocationPointer();
    		
    		// write a bogus rollback so that spNow can be replayed
    		this.getGobbler().logMessage("nothing");

    		hbaseService.truncateTable(id, spNow);
    	}
    }
    
    public Collection<GTable> getTables() {
        return this.tableById.values();
    }
    
    /**
     * namespace comparison is case insensitive
     * 
     * @param namespace
     * @return not nullable
     */
    public Collection<GTable> getTables(String namespace) {
        List<GTable> list = new ArrayList<>();
        for (GTable i:this.tableById.values()) {
            if (i.getNamespace().equalsIgnoreCase(namespace)) {
                list.add(i);
            }
        }
        return list;
    }
    
    public GTable getTable(String ns, int id) {
        return getTable(id);
    }

    public GTable getTable(int id) {
        if (id == Integer.MAX_VALUE) {
            return this.sysmeta;
        }
        GTable table = this.tableById.get(id);
        return table;
    }

    public TrxMan getTrxMan() {
        return this.trxMan;
    }

    /**
     * commit a transaction
     * 
     * @param trxid transaction id, must be unique
     * @param trxts transaction timestamp, must be unique and incremental. this value is also used to version the 
     * record
     */
    public void commit(long trxid, long trxts) {
        this.trxMan.commit(trxid, trxts);
        if (this.gobbler != null) {
            this.gobbler.logCommit(trxid, trxts);
        }
    }

    public void rollback(long trxid) {
        this.trxMan.rollback(trxid);
        if (this.gobbler != null) {
            this.gobbler.logRollback(trxid);
        }
    }

    void recover() throws Exception {
        long sp = this.spaceman.getAllocationPointer();
        if (!cp.isDatabaseOpen()) {
        	return;
        }
        _log.warn("database wasn't shutdown gracefully. recovery is required");
        cp.setDatabaseOpen(true);
    	Recoverer recoverer = new Recoverer(this, this.gobbler);
    	recoverer.run();
    	this.trxMan.close();
    	this.sysmeta.getMemTable().render(Long.MIN_VALUE);
    	for (GTable table:this.tableById.values()) {
    	    table.getMemTable().render(Long.MIN_VALUE);
    	}
        if (sp != this.spaceman.getAllocationPointer()) {
        	_log.error("unpexpected write operation detected {} {}", sp, this.spaceman.getAllocationPointer());
        	throw new CodingError();
        }
        this.trxMan.reset();
    }
    
    /**
     * recover transactions for hbase
     * 
     * @throws Exception
     */
    private void recoverTrx() throws Exception {
    	if (this.hbaseService == null) {
    		return;
    	}
    	if (this.gobbler.getPersistencePointer().get() <= this.gobbler.getStartSp()) {
    		// system not initialized
    		return;
    	}
    	TrxRecoverer recoverer = new TrxRecoverer();
    	recoverer.run(this.trxMan, this.gobbler, this.hbaseService.getCurrentSP());
	}

    /**
     * synchronize meta data and storage 
     * @throws ClassNotFoundException 
     */
    void sync() throws ClassNotFoundException {
        if (this.sysmeta == null) {
            return;
        }
        Set<Integer> validIds = new HashSet<Integer>();
        for (RowIterator i = this.sysmeta.scan(1, 1); i.next();) {
            long pRow = i.getRowPointer();
            if (pRow == 0) break;
            SysMetaRow row = new SysMetaRow(SlowRow.from(Row.fromSpacePointer(this.spaceman, pRow, 0)));
            int tableId = row.getTableId();
            if (getTable(tableId) == null) {
                String ns = row.getNamespace();
                createTable(ns, row.getTableName(), tableId, false, row.getType());
            }
            validIds.add(tableId);
        }
        for (Integer i:new ArrayList<Integer>(this.tableById.keySet())) {
            if (!validIds.contains(i)) {
                this.tableById.remove(i);
            }
        }
    }
    
    public Gobbler getGobbler() {
    	return this.gobbler;
    }

	public final SpaceManager getSpaceManager() {
		return this.spaceman;
	}

	public void setLastClosedTransactionId(long trxid) {
		this.lastClosedTrxId = trxid;
	}

	/**
	 * the most recent finished transaction, either committed or rolled back
	 * @return
	 */
	public long getLastClosedTransactionId() {
		return this.lastClosedTrxId;
	}
	
	/**
	 * get the highest sp found in the humpback. this value is only meaningful when the system is shutdown
	 * 
	 * @return
	 * @throws Exception 
	 */
	public long getLatestSP() {
		return this.gobbler.getLatestSp();
    }

	public void disableShutdownHook() {
		Runtime.getRuntime().removeShutdownHook(this.hook);
	}

	private boolean validate() {
		if (!this.config.isValidationOn()) {
			return true;
		}
		boolean result = true;
		result = result && this.sysmeta.validate();
		for (GTable i:this.tableById.values()) {
			result = result && i.validate();
		}
		return result;
	}

	public CheckPoint getCheckPoint() {
		return this.cp;
	}

	public ConfigService getConfig() {
		return this.config;
	}
	
	public HBaseStorageService getHBaseService() {
		return this.hbaseService;
	};
	
	public long getServerId() {
		return this.cp.getServerId();
	}
	
	public SysMetaRow getTableInfo(int tableId) {
		Row row = this.sysmeta.getRow(1, 1, KeyMaker.make(tableId));
    	return new SysMetaRow(SlowRow.from(row));
	}

	public RowLockMonitor getRowLockMonitor() {
		return this.rowLockMonitor ;
	}
	
	public FishJobManager getJobManager() {
		return this.jobman;
	}
}
