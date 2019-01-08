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

import static com.antsdb.saltedfish.util.UberFormatter.hex;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.MemoryManager;
import com.antsdb.saltedfish.minke.Minke;
import com.antsdb.saltedfish.minke.MinkeCache;
import com.antsdb.saltedfish.minke.PageCacheWarmer;
import com.antsdb.saltedfish.slave.JdbcReplicator;
import com.antsdb.saltedfish.slave.SlaveReplicator;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.vdm.KeyMaker;
import com.antsdb.saltedfish.storage.HBaseStorageService;
import com.antsdb.saltedfish.storage.KerberosHelper;
import com.antsdb.saltedfish.util.CodingError;
import com.antsdb.saltedfish.util.FishJobManager;
import com.antsdb.saltedfish.util.LatencyDetector;
import com.antsdb.saltedfish.util.LongLong;
import com.antsdb.saltedfish.util.UberTime;
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
 * <li>Table/Namespace is preserved in case sensitive way as specified from
 * input</li>
 * <li>Name conflict is detected in a case insensitive way, meaning Customer
 * table and customer table cannot both exist in the system</li>
 * </ul>
 * 
 * @author *-xguo0<@
 *
 */
public final class Humpback {
    public final static Character NULL = Character.MIN_VALUE;
    public final static int SYSMETA_TABLE_ID = 0;
    public final static int SYSNS_TABLE_ID = 1;
    public final static int SYSSTATS_TABLE_ID = 2;
    public final static int SYSCOLUMN_TABLE_ID = 3;
    public final static int SYSCONFIG_TABLE_ID = 4;
    public final static int SYSTABLE_MAX_ID = 0xf;
    public final static String SYS_NAMESAPCE = "ANTSDB";

    static List<Humpback> _instances = Collections.synchronizedList(new ArrayList<>());
    static Logger _log = UberUtil.getThisLogger();

    File home;
    File data;
    ConcurrentMap<Integer, GTable> tableById = new ConcurrentHashMap<>();
    ConcurrentMap<String, String> namespaces = new ConcurrentHashMap<String, String>();
    boolean isClosed = true;
    TrxMan trxMan;
    CheckPoint cp;
    GTable sysmeta;
    GTable sysns;
    GTable sysstats;
    GTable syscolumn;
    GTable sysconfig;
    SysMetaRow sysmetaTableInfo;
    SysMetaRow sysnsTableInfo;
    SysMetaRow sysstatsTableInfo;
    SysMetaRow syscolumnTableInfo;
    SysMetaRow sysconfigTableInfo;
    Gobbler gobbler;
    SpaceManager spaceman;
    HBaseStorageService hbaseService = null;
    FishJobManager jobman = new FishJobManager();
    GarbageCollector gc = new GarbageCollector();
    StorageEngine storage;

    private Carbonfreezer carbonfreezer;
    private ScheduledFuture<?> carbonfreezerFuture;
    private ShutdownThread hook;
    private ConfigService config;
    private RowLockMonitor rowLockMonitor = new RowLockMonitor();
    private volatile long lastClosedTrxId;
    private boolean forceDisableHBase;
    private Synchronizer synchronizer;
    private boolean isMutable = true;
    private CacheEvictor cacheEvictor;
    private ScheduledFuture<?> cacheEvictorFuture;
    private Set<HumpbackSession> sessions = ConcurrentHashMap.newKeySet();
    private int tabletSize = 64 * 1024 * 1024;
    private Replicator<Statistician> statisticianThread;
    private Replicator<JdbcReplicator> slaveThread;
    private TotalLogDependency logDependency = new TotalLogDependency();
    private long serverId;
    private PageCacheWarmer warmer;

    class ShutdownThread extends Thread {
        @Override
        public void run() {
            shutdown();
        }
    }

    public Humpback(File dbfolder) throws Exception {
        this(dbfolder, false);
    }

    public Humpback(File dbfolder, boolean forceDisableHBase) {
        this.home = dbfolder;
        this.data = new File(dbfolder, "data");
        this.forceDisableHBase = forceDisableHBase;
    }

    public void setMutable(boolean value) {
        if (this.cp != null) {
            throw new IllegalArgumentException("mutable can only be set before calling open()");
        }
        this.isMutable = value;
    }

    public boolean isMutable() {
        return this.isMutable;
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
        this.tabletSize = this.config.getTabletSize();

        // create database folder if absent

        _log.info("Humpback home: " + this.home.getAbsolutePath());
        if (!data.isDirectory()) {
            _log.info("data directory is not found, creating one");
            data.mkdirs();
        }
        File sys = new File(data, SYS_NAMESAPCE);
        if (!sys.exists()) {
            sys.mkdirs();
        }

        // initialize

        this.cp = new CheckPoint(new File(this.data, "checkpoint.bin"), this.isMutable);
        this.cp.open();
        this.serverId = this.config.getServerId() >= 0 ? this.config.getServerId() : this.cp.getServerId();
        _log.info("server id: {}", this.serverId);
        boolean isRecovering = false;
        if (this.isMutable) {
            isRecovering = this.cp.isDatabaseOpen();
        }
        init(isRecovering);

        // add shutdown hook

        this.hook = new ShutdownThread();
        Runtime.getRuntime().addShutdownHook(this.hook);
        this.isClosed = false;
        _instances.add(this);
        
        // start slave replicator
        
        if ("true".equals(getConfig(SlaveReplicator.KEY_ENABLED))) {
            try {
                startSlave();
            }
            catch (Exception x) {
                _log.error("unable to start job", x);
            }
        }
        
        // misc.
        
        addLogDependency(new TabletLogDependency(this));
        addLogDependency(new StorageLogDependency(this));
        addLogDependency(new SlaveLogDependency(this));
    }

    void init(boolean isRecovering) throws Exception {
        if (!this.data.isDirectory()) {
            throw new HumpbackException("home director is not found");
        }
        this.carbonfreezer = new Carbonfreezer(this);

        // system wide parameters

        HumpbackUtil._isFakeDeletionEnabled = this.config.isFakeDeletetionEnabled();

        // init space

        this.spaceman = new SpaceManager(this.data, this.isMutable, config.getSpaceFileSize());
        _log.info("log retention: {}", this.config.getLogRetentionStrategy());
        this.spaceman.setLogRetention(this.config.getLogRetentionStrategy());
        this.spaceman.open();
        this.trxMan = new TrxMan(this.spaceman);

        // init logger

        this.gobbler = new Gobbler(this.spaceman, config.isLogWriterEnabled());

        // krb login
        KerberosHelper.initialize(this.config.getKrbRealm(), this.config.getKrbKdc(), this.config.getKrbJaasConf());

        // storage engine

        initStorage();

        // initialize sysmeta table

        initSysmeta(isRecovering);
        
        // initialize tables

        initTables(isRecovering);

        // data recovery

        if (this.isMutable) {
            recover();
        }

        // initialize name spaces. this step must be executed after recover()
        
        initNamespaces();
        
        // marking database is open

        if (this.isMutable) {
            this.cp.setDatabaseOpen(true);
        }

        // validate

        if (this.isMutable) {
            if (!validate()) {
                throw new OrcaException("failed in validation");
            }
        }

        // background jobs

        initJobs();
        
        // setting latency detection
        
        LatencyDetector.set(this.config.getLatencyDetectionMs());
    }

    private void initStorage() throws Exception {
        String engine = this.config.getStorageEngineName();
        if ("hbase".equals(engine)) {
            this.hbaseService = new HBaseStorageService(this);
            this.hbaseService.open(this.home, config, this.isMutable);
            MinkeCache cache = new MinkeCache(this.hbaseService);
            this.storage = cache;
            this.storage.open(new File(this.home, "cache"), config, this.isMutable);
        }
        else if ("minke".equals(engine)) {
            Minke minke = new Minke();
            this.storage = minke;
            this.storage.open(new File(this.home, "data"), this.config, this.isMutable);
        }
        else if ("minke-cache".equals(engine)) {
            Minke minke = new Minke();
            minke.open(new File(this.home, "data"), this.config, this.isMutable);
            MinkeCache cache = new MinkeCache(minke);
            this.storage = cache;
            this.storage.open(new File(this.home, "cache"), config, this.isMutable);
        }
        else {
            throw new OrcaException("invalid engine name", engine);
        }
    }

    private void initJobs() throws Exception {
        if (!this.isMutable) {
            return;
        }
        
        // start statistician
        
        this.statisticianThread = new Replicator<>("statistician", this, new Statistician(this), false);
        this.statisticianThread.start();
        
        // start synchronizer
        
        if ((getStorageEngine() instanceof Minke) || (getStorageEngine() instanceof MinkeCache)) {
            this.synchronizer = new Synchronizer(this);
        }
        
        // start cache evictor
        
        if (getStorageEngine() instanceof MinkeCache) {
            this.cacheEvictor = new CacheEvictor((MinkeCache) getStorageEngine(), config.getCacheEvictorTarget());
        }
        
        // start delayed jobs
        
        this.jobman.schedule(10, TimeUnit.SECONDS, () -> {
            initDelayedJobs();
        });
        
        // start warmer
        
        long warmSize = this.config.getWarmerSize();
        if (warmSize > 0) {
            Minke minke;
            if (this.storage instanceof Minke) {
                minke = (Minke)this.storage;
            }
            else if (this.storage instanceof MinkeCache) {
                minke = ((MinkeCache)this.storage).getMinke();
            }
            else {
                throw new IllegalArgumentException();
            }
            this.warmer = new PageCacheWarmer(minke, warmSize);
            this.jobman.scheduleWithFixedDelay(5, TimeUnit.MINUTES, this.warmer);
        }
    }

    private void initDelayedJobs() {
        if (this.isClosed) {
            return;
        }
        this.carbonfreezerFuture = this.jobman.scheduleWithFixedDelay(this.carbonfreezer, 2, TimeUnit.MINUTES);
        if (this.config.isSynchronizerEnabled() && (this.synchronizer != null)) {
            this.synchronizer.start();
        }
        if (this.cacheEvictor != null) {
            this.cacheEvictorFuture = this.jobman.scheduleWithFixedDelay(this.cacheEvictor, 10, TimeUnit.SECONDS);
        }
    }
    
    private void initSysmeta(boolean isRecovering) throws IOException {
        this.sysmetaTableInfo = createMetaRow(SYSMETA_TABLE_ID);
        this.sysnsTableInfo = createMetaRow(SYSNS_TABLE_ID);
        this.sysstatsTableInfo = createMetaRow(SYSSTATS_TABLE_ID);
        this.syscolumnTableInfo = createMetaRow(SYSCOLUMN_TABLE_ID);
        this.sysconfigTableInfo = createMetaRow(SYSCONFIG_TABLE_ID);

        if (this.isMutable) {
            seedCreateStorgeTable(this.sysmetaTableInfo);
            seedCreateStorgeTable(this.sysnsTableInfo);
            seedCreateStorgeTable(this.sysstatsTableInfo);
            seedCreateStorgeTable(this.syscolumnTableInfo);
            seedCreateStorgeTable(this.sysconfigTableInfo);
        }
        this.storage.syncTable(this.sysmetaTableInfo);
        this.storage.syncTable(this.sysnsTableInfo);
        this.storage.syncTable(this.sysstatsTableInfo);
        this.storage.syncTable(this.syscolumnTableInfo);
        this.storage.syncTable(this.sysconfigTableInfo);
        this.sysmeta = createGtable(this.sysmetaTableInfo, isRecovering);
        this.sysns = createGtable(this.sysnsTableInfo, isRecovering);
        this.sysstats = createGtable(this.sysstatsTableInfo, isRecovering);
        this.syscolumn = createGtable(this.syscolumnTableInfo, isRecovering);
        this.sysconfig = createGtable(this.sysconfigTableInfo, isRecovering);
    }

    private GTable createGtable(SysMetaRow meta, boolean isRecovering) throws IOException {
        GTable gtable = new GTable(this, meta.getNamespace(), meta.getTableId(), this.tabletSize, TableType.DATA);
        gtable.setMutable(this.isMutable);
        gtable.memtable.setRecoveryMode(isRecovering);
        gtable.open();
        this.tableById.put(gtable.id, gtable);
        return gtable;
    }
    
    private void seedCreateStorgeTable(SysMetaRow info) {
        if (!this.storage.exist(info.getTableId())) {
            this.storage.createTable(info);
        }
    }

    private SysMetaRow createMetaRow(int id) {
        SysMetaRow row = new SysMetaRow(id);
        row.setNamespace(SYS_NAMESAPCE);
        row.setTableId(id);
        row.setTableName(String.format("x%x", id));
        row.setType(TableType.DATA);
        return row;
    }
    
    private void initNamespaces() throws Exception {
        this.namespaces.put(SYS_NAMESAPCE.toLowerCase(), SYS_NAMESAPCE);
        for (RowIterator i = this.sysns.scan(1, 1, true);i.next();) {
            SysNamespaceRow row = new SysNamespaceRow(i.getRow());
            this.namespaces.put(row.getNamespace().toLowerCase(), row.getNamespace());
            File nsfile = new File(this.data, row.getNamespace());
            if (!nsfile.exists()) {
                if (!nsfile.mkdir()) {
                    throw new HumpbackException("unable to make dir: " + nsfile);
                }
            }
        }
    }
    
    private void initTables(boolean isRecovering) throws Exception {
        for (RowIterator i = this.sysmeta.scan(1, 1, true);i.next();) {
            SysMetaRow row = new SysMetaRow(SlowRow.from(i.getRow()));
            this.storage.syncTable(row);
            if (row.isDeleted()) {
                continue;
            }
            int id = row.getTableId();
            String ns = row.getNamespace();
            this.storage.syncTable(row);
            GTable gtable = new GTable(this, ns, id, this.tabletSize, row.getType());
            gtable.setMutable(this.isMutable);
            gtable.memtable.setRecoveryMode(isRecovering);
            gtable.open();
            this.tableById.put(gtable.getId(), gtable);
        }
    }

    private void closeJobs() {
        // terminate back-end jobs

        try {
            if (this.carbonfreezerFuture != null) {
                this.carbonfreezerFuture.cancel(true);
                this.carbonfreezerFuture = null;
            }
            if (this.statisticianThread != null) {
                this.statisticianThread.close();
                this.statisticianThread = null;
            }
            if (this.slaveThread != null) {
                this.slaveThread.close();
                this.slaveThread = null;
            }
            if (this.synchronizer != null) {
                this.synchronizer.close(true);
                this.synchronizer = null;
            }
            if (this.cacheEvictorFuture != null) {
                this.cacheEvictorFuture.cancel(true);
                this.cacheEvictorFuture = null;
            }
            if (this.jobman != null) {
                this.jobman.close();
            }
            if (this.slaveThread != null) {
                this.slaveThread.close();
            }
        }
        catch (Exception e) {
            _log.warn("errors shutting down carbonfreezer", e);
        }
        // close all jobs

        this.jobman.close();
    }
    
    /**
     * for testing purpose
     */
    void crash() {
        if (this.isClosed) {
            return;
        }
        this.isClosed = true;
        
        // close jobs
        
        closeJobs();

        // close all tables

        for (GTable i : this.tableById.values()) {
            i.close();
        }
        
        // close minke

        try {
            getStorageEngine().close();
        }
        catch (Exception x) {
            _log.warn("errors shutting down ", x);
        }

        // close check point file

        this.cp.close();

        // memory report

        _instances.remove(this);
        if (_instances.size() == 0) {
            MemoryManager.report();
        }

        // all done

        _log.info("humpback crashed peacefully");
    }
    
    public synchronized void shutdown() {
        if (this.isClosed) {
            return;
        }
        this.isClosed = true;

        closeJobs();
        
        // close write ahead logger

        if (this.gobbler != null) {
            this.gobbler.close();
        }
        if (this.spaceman != null) {
            this.spaceman.close();
        }

        // close trxman

        this.lastClosedTrxId = this.trxMan.getNewTrxId();
        this.trxMan.close();

        // write all data files

        try {
            _log.info("shutting down humpback...");
            if (this.isMutable) {
                flush(Long.MIN_VALUE, true);
                tableMustBeCarbonfreezed(this.sysmeta);
                for (GTable i : this.tableById.values()) {
                    tableMustBeCarbonfreezed(i);
                }
            }
        }
        catch (Exception x) {
            _log.error("failed to shutdown", x);
        }

        // validate

        if (this.isMutable) {
            validate();
        }

        // final gc

        for (long time = UberTime.getTime();;) {
            if (UberTime.getTime() != time) {
                gc(UberTime.getTime());
                break;
            }
        }

        // close all tables

        for (GTable i : this.tableById.values()) {
            i.close();
        }

        // close minke

        try {
            getStorageEngine().close();
        }
        catch (Exception x) {
            _log.warn("errors shutting down ", x);
        }

        // mark database properly closed

        if (this.isMutable) {
            this.cp.setDatabaseOpen(false);
        }
        this.cp.close();

        // memory report

        _instances.remove(this);
        if (_instances.size() == 0) {
            MemoryManager.report();
        }

        // all done

        _log.info("humpback stopped peacefully");
    }

    private void tableMustBeCarbonfreezed(GTable gtable) {
        for (MemTablet i : gtable.memtable.getTablets()) {
            if (!i.isCarbonfrozen()) {
                _log.error("tablet {} is not carbonfreezed", i);
            }
        }
    }

    public void flush(long oldestTrxid, boolean force) throws Exception {
        _log.info("flushing...");

        // save system meta

        if (this.sysmeta != null) {
            this.sysmeta.memtable.carbonfreeze(oldestTrxid, force);
        }

        // save normal table

        for (Map.Entry<Integer, GTable> i : this.tableById.entrySet()) {
            GTable table = i.getValue();
            table.memtable.carbonfreeze(oldestTrxid, force);
        }
    }

    public synchronized void createNamespace(HumpbackSession hsession, String name) throws HumpbackException {
        String key = name.toLowerCase();
        if (this.namespaces.get(key) != null) {
            throw new HumpbackException("namespace folder already exists");
        }
        // Create name space in HBase

        this.storage.createNamespace(name);

        // create folder
        
        File nsfolder = new File(this.data, name);
        if (!nsfolder.exists()) {
            if (!nsfolder.mkdirs()) {
                this.namespaces.remove(key);
                throw new HumpbackException("failed to create namespace folder");
            }
        }

        // remember it in metadata
        
        this.namespaces.putIfAbsent(key, name);
        SysNamespaceRow meta = new SysNamespaceRow(name);
        this.sysns.put(hsession, 1, meta.row, 0);
    }

    public void dropNamespace(HumpbackSession hsession, String name) throws HumpbackException {
        String key = name.toLowerCase();
        String realName = this.namespaces.remove(key);
        if (realName == null) {
            throw new HumpbackException("namespace doesn't exist: " + name);
        }
        for (GTable i : getTables(name)) {
            dropTable(hsession, name, i.getId());
        }
        File nsfolder = new File(this.data, realName);
        for (File i : nsfolder.listFiles()) {
            i.delete();
        }
        nsfolder.delete();

        // remove namespace from HBase

        this.storage.deleteNamespace(realName);
        
        // remove it from metadata
        
        this.sysns.delete(hsession, 1, KeyMaker.make(key), 0);
    }

    public String getNamespace(String ns) {
        return this.namespaces.get(ns.toLowerCase());
    }

    public List<String> getNamespaces() {
        return new ArrayList<String>(this.namespaces.values());
    }

    public synchronized GTable createTable(HumpbackSession hsession, String ns, String name, int id, TableType type) {
        return createTable(hsession, ns, name, id, true, type);
    }

    private synchronized GTable createTable(HumpbackSession hsession,  
                                            String ns, 
                                            String name, 
                                            int id, 
                                            boolean createMeta, 
                                            TableType type) {
        // check id rule
        
        if (id == SYSMETA_TABLE_ID) {
            throw new IllegalArgumentException();
        }
        
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
            gtable = new GTable(this, ns, id, this.tabletSize, type);
            gtable.setMutable(this.isMutable);
            tableById.put(id, gtable);

            // create table in HBase

            SysMetaRow meta = new SysMetaRow(id);
            meta.setTableId(id);
            meta.setNamespace(ns);
            meta.setTableName(name);
            meta.setType(type);
            this.storage.createTable(meta);

            // update meta data

            if (createMeta) {
                this.sysmeta.put(hsession, 1, meta.row, 0);
                this.storage.syncTable(meta);
            }

            return gtable;
        }
        catch (Exception x) {
            throw new HumpbackException(x);
        }
    }

    public synchronized void dropTable(HumpbackSession hsession, String ns, int id) throws HumpbackException {
        GTable table = this.tableById.get(id);
        SysMetaRow meta = getTableInfo(id);
        if (table == null || meta == null) {
            _log.warn("table {} doesn't exist", id);
            return;
        }

        // drop table in HBase

        this.storage.deleteTable(id);

        // update meta data

        table.drop();
        this.tableById.remove(id);
        meta.setDeleted(true);
        this.sysmeta.put(hsession, 1, meta.row, 1000);
        this.storage.syncTable(meta);
    }

    public synchronized void truncateTable(HumpbackSession hsession, int oldTableId, int newTableId) 
    throws HumpbackException {
        SysMetaRow oldTable = getTableInfo(oldTableId);
        if (oldTable == null) {
            throw new HumpbackException("table is not found: " + oldTableId);
        }

        // create new table

        dropTable(hsession, oldTable.getNamespace(), oldTableId);
        createTable(hsession, oldTable.getNamespace(), oldTable.getTableName(), newTableId, TableType.DATA);
        
        // copy the columns to the new table
        
        List<HColumnRow> columns = getColumns(oldTableId);
        for (HColumnRow i:columns) {
            if (i.isDeleted()) {
                continue;
            }
            HColumnRow ii = new HColumnRow(newTableId, i.getColumnPos());
            ii.setColumnName(i.getColumnName());
            this.syscolumn.put(hsession, 0, ii.row, 0);
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
        for (GTable i : this.tableById.values()) {
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
        GTable table = this.tableById.get(id);
        return table;
    }

    public TrxMan getTrxMan() {
        return this.trxMan;
    }

    /**
     * commit a transaction
     * 
     * @param trxid
     *            transaction id, must be unique
     * @param trxts
     *            transaction timestamp, must be unique and incremental. this
     *            value is also used to version the record
     */
    public void commit(HumpbackSession hsession, long trxid, long trxts) {
        if (this.gobbler != null) {
            this.gobbler.logCommit(hsession, trxid, trxts);
        }
        this.trxMan.commit(trxid, trxts);
    }

    public void rollback(HumpbackSession hsession, long trxid) {
        if (this.gobbler != null) {
            this.gobbler.logRollback(hsession, trxid);
        }
        this.trxMan.rollback(trxid);
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
        for (GTable table : this.tableById.values()) {
            table.getMemTable().render(Long.MIN_VALUE);
        }
        if (sp != this.spaceman.getAllocationPointer()) {
            _log.error("unpexpected write operation detected {} {}", sp, this.spaceman.getAllocationPointer());
            throw new CodingError();
        }
        this.trxMan.reset();
    }

    /**
     * recover transactions for hbase. not needed anymore because Replicator can recover transaction now
     * 
     * @throws Exception
     */
    @SuppressWarnings("unused")
    private void recoverTrx() throws Exception {
        if (this.gobbler.getPersistencePointer().get() <= this.gobbler.getStartSp()) {
            // system not initialized
            return;
        }
        long lp = getStatistician().getReplicateLogPointer();
        if (this.storage.getReplicable() != null) {
            lp = Math.min(lp, this.storage.getReplicable().getReplicateLogPointer());
        }
        if (lp == 0) {
            return;
        }
        if (getSlave() != null) {
            lp = Math.min(lp, getSlave().getReplicateLogPointer());
        }
        TrxRecoverer recoverer = new TrxRecoverer();
        recoverer.run(this.trxMan, this.gobbler, lp);
    }

    /**
     * synchronize meta data and storage
     * 
     * @throws ClassNotFoundException
     * @throws IOException
     */
    void recoverTable() throws ClassNotFoundException, IOException {
        if (this.sysmeta == null) {
            return;
        }
        Set<Integer> validIds = new HashSet<Integer>();
        for (RowIterator i = this.sysmeta.scan(1, 1, true); i.next();) {
            SlowRow srow = SlowRow.from(i.getRow());
            if (srow == null) {
                continue;
            }
            SysMetaRow row = new SysMetaRow(srow);
            if (row.isDeleted()) {
                continue;
            }
            int tableId = row.getTableId();
            if (getTable(tableId) == null) {
                String ns = row.getNamespace();
                TableType type = row.getType();
                GTable gtable = new GTable(this, ns, row.getTableId(), this.tabletSize, type);
                gtable.setMutable(isMutable);
                gtable.memtable.setRecoveryMode(true);
                gtable.open();
                this.tableById.put(gtable.getId(), gtable);
                if (this.storage.getTable(gtable.getId()) == null) {
                    this.storage.createTable(row);
                }
            }
            validIds.add(tableId);
        }
        for (Map.Entry<Integer, GTable> i : this.tableById.entrySet()) {
            if (i.getKey() <= SYSTABLE_MAX_ID) {
                continue;
            }
            if (!validIds.contains(i.getKey())) {
                this.tableById.remove(i.getKey());
                i.getValue().drop();
                this.gc.collect(UberTime.getTime() + 1);
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
     * 
     * @return
     */
    public long getLastClosedTransactionId() {
        return this.lastClosedTrxId;
    }

    /**
     * get the highest sp found in the humpback. this value is only meaningful
     * when the system is shutdown
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
        for (GTable i : this.tableById.values()) {
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
        return this.serverId;
    }

    public SysMetaRow getTableInfo(int tableId) {
        if (tableId == this.sysmetaTableInfo.getTableId()) {
            return this.sysmetaTableInfo;
        }
        if (tableId == this.sysnsTableInfo.getTableId()) {
            return this.sysnsTableInfo;
        }
        if (tableId == this.sysstatsTableInfo.getTableId()) {
            return this.sysstatsTableInfo;
        }
        if (tableId == this.syscolumnTableInfo.getTableId()) {
            return this.syscolumnTableInfo;
        }
        if (tableId == this.sysconfigTableInfo.getTableId()) {
            return this.sysconfigTableInfo;
        }
        Row row = this.sysmeta.getRow(1, 1, KeyMaker.make(tableId));
        if (row == null) {
            return null;
        }
        return new SysMetaRow(SlowRow.from(row));
    }

    public RowLockMonitor getRowLockMonitor() {
        return this.rowLockMonitor;
    }

    public FishJobManager getJobManager() {
        return this.jobman;
    }

    Minke getMinke() {
        if (this.storage instanceof Minke) {
            return (Minke) this.storage;
        }
        else {
            return null;
        }
    }

    public void carbonfreeze(long oldestTrxId, boolean force) throws IOException {
        this.carbonfreezer.run(oldestTrxId, force);
    }

    /**
     * collect garbages that are marked older than the specified timestamp
     * 
     * @param ts
     */
    public void gc(long ts) {
        this.gc.collect(ts);
        getStorageEngine().gc(ts);
    }

    public void gc() {
        UberTime.step();
        gc(UberTime.getTime());
    }
    
    /**
     * free unused resource
     */
    public synchronized void recycle() {
        LongLong span = this.storage.getLogSpan();
        long ts = getOldestQueryTimestamp();
        _log.trace("start humpback recycling with ts={} minke sp={}", ts, hex(span.y));

        // free tablets

        for (GTable table : this.tableById.values()) {
            table.memtable.free(span.y);
        }

        // free logs

        long sp = this.logDependency.getLogPointer();
        _log.trace("start log space recycling with sp={}", hex(sp));
        this.spaceman.gc(this.gc, sp);
        _log.trace("log space recycling is finished");

        //

        _log.trace("start garbage collection");
        this.gc.collect(ts);
        if (getStorageEngine() != null) {
            getStorageEngine().gc(ts);
        }
        _log.trace("garbage collection is finished");

        _log.trace("recycling is finsihed");
    }

    public List<SysMetaRow> getTablesMeta() {
        List<SysMetaRow> result = new ArrayList<>();
        RowIterator it = this.sysmeta.scan(0, Long.MAX_VALUE, true);
        while (it.next()) {
            SysMetaRow row = new SysMetaRow(SlowRow.from(it.getRow()));
            result.add(row);
        }
        return result;
    }

    public StorageEngine getStorageEngine() {
        return this.storage;
    }

    public CacheEvictor getCacheEvictor() {
        return this.cacheEvictor;
    }

    public HumpbackSession createSession(String endpoint) {
        HumpbackSession result = new HumpbackSession(endpoint);
        this.sessions.add(result);
        return result;
    }

    public void deleteSession(HumpbackSession hsession) {
        this.sessions.remove(hsession);
    }

    public Collection<HumpbackSession> getSessions() {
        return this.sessions;
    }
    
    /**
     * get the oldest timestamp of all queries executed in all sessions
     * 
     * @return Long.MAX_VALUE if there is active queries
     */
    public long getOldestQueryTimestamp() {
        long result = Long.MAX_VALUE;
        for (HumpbackSession session : this.sessions) {
            long ts = session.ts;
            if (ts == 0) {
                continue;
            }
            result = Math.min(result, ts);
        }
        return result;
    }

    public GTable findTable(String ns, String name) {
        for (SysMetaRow i : getTablesMeta()) {
            if (i.isDeleted()) {
                continue;
            }
            if (!i.getNamespace().equalsIgnoreCase(ns)) {
                continue;
            }
            if (!i.getTableName().equalsIgnoreCase(name)) {
                continue;
            }
            GTable table = getTable(i.getTableId());
            return table;
        }
        return null;
    }
    
    public Synchronizer getSynchronizer() {
        return this.synchronizer;
    }
    
    public Statistician getStatistician() {
        return this.statisticianThread.getReplicable();
    }

    public HColumnRow addColumn(HumpbackSession hsession, long trxid, int tableId, String name) {
        if (getTableInfo(tableId) == null) {
            throw new HumpbackException("table {} not found", tableId);
        }
        int maxColumnPos = 0;
        for (HColumnRow i:getColumns(tableId)) {
            maxColumnPos = Math.max(i.getColumnPos(), maxColumnPos);
            if (i.isDeleted()) {
                continue;
            }
            if (i.getColumnName().equals(name)) {
                throw new HumpbackException("duplicate column name {} for table {}", name, tableId);
            }
        }
        HColumnRow result = new HColumnRow(tableId, maxColumnPos + 1);
        result.setColumnName(name);
        this.syscolumn.put(hsession, trxid, result.row, 0);
        return result;
    }
    
    public void addColumn(HumpbackSession hsession, long trxid, int tableId, int columnId, String name) {
        HColumnRow row = new HColumnRow(tableId, columnId);
        row.setColumnName(name);
        this.syscolumn.put(hsession, trxid, row.row, 0);
    }

    public void deleteColumn(HumpbackSession hsession, long trxid, int tableId, int columnId) {
        byte[] key = KeyMaker.gen(tableId, columnId);
        Row row = this.syscolumn.getRow(trxid, Long.MAX_VALUE, key);
        if (row == null) {
            throw new HumpbackException("column is not found " + tableId + ":" + columnId);
        }
        HColumnRow column = new HColumnRow(SlowRow.from(row));
        column.setDeleted(true);
        this.syscolumn.put(hsession, trxid, column.row, 0);
    }
    
    public List<HColumnRow> getColumns(int tableId) {
        if (getTableInfo(tableId) == null) {
            return null;
        }
        List<HColumnRow> result = new ArrayList<>();
        byte[] from = KeyMaker.gen(tableId, 0);
        byte[] to = KeyMaker.gen(tableId+1, 0);
        long options = ScanOptions.excludeEnd(0);
        RowIterator i = this.syscolumn.scan(0, Long.MAX_VALUE, from, to, options);
        while (i.next()) {
            HColumnRow row = new HColumnRow(SlowRow.from(i.getRow()));
            result.add(row);
        }
        return result;
    }
    
    public Replicator<JdbcReplicator> getSlaveReplicator() {
        return this.slaveThread;
    }

    public void setConfig(HumpbackSession hsession, String key, Object value) {
        SysConfigRow row = new SysConfigRow(key);
        if (value == null) {
            this.sysconfig.delete(hsession, 1, row.row.getKey(), 0);
            return;
        }
        row.setValue(value.toString());
        row.row.setTrxTimestamp(this.trxMan.getNewVersion());
        this.sysconfig.put(hsession, 1, row.row, 0);
    }
    
    public Map<String,String> getAllConfig() {
        Map<String,String> result = new HashMap<>();
        for (RowIterator i=this.sysconfig.scan(0, Integer.MAX_VALUE, true); i.next();) {
            Row row = i.getRow();
            SysConfigRow ii = new SysConfigRow(SlowRow.from(row));
            result.put(ii.getKey(), ii.getVale());
        }
        return result;
    }
    
    public String getConfig(String key) {
        Row row = this.sysconfig.getRow(0, Long.MAX_VALUE, KeyMaker.make(key));
        if (row == null) {
            return null;
        }
        SysConfigRow result = new SysConfigRow(SlowRow.from(row));
        return result.getVale();
    }
    
    public JdbcReplicator getSlave() {
        return this.slaveThread != null ? this.slaveThread.getReplicable() : null;
    }
    
    public synchronized void startSlave() throws Exception {
        if (this.isClosed) {
            throw new HumpbackException("database is closed");
        }
        if (this.slaveThread != null) {
            throw new HumpbackException("slave already started");
        }
        this.slaveThread = new Replicator<>("slave", this, new SlaveReplicator(this), true);
        this.slaveThread.start();
    }
    
    public synchronized void stopSlave() {
        if (this.slaveThread == null) {
            throw new HumpbackException("slave not started");
        }
        this.slaveThread.close();
        this.slaveThread = null;
    }

    public Long getConfigAsLong(String key, long defaultValue) {
        String value = getConfig(key);
        return (value != null) ? Long.parseLong(value) : defaultValue;
    }
    
    public boolean getConfigAsBoolean(String key, boolean defaultValue) {
        String value = getConfig(key);
        return (value != null) ? Boolean.parseBoolean(value) : defaultValue;
    }
    
    public void addLogDependency(LogDependency value) {
        this.logDependency.logDependency.add(value);
    }
    
    public LogDependency getLogDependency() {
        return this.logDependency;
    }

    public String getEndpoint() {
        try {
            String port = getConfig().getProperty("fish.port", "3306");
            String endpoint;
            endpoint = InetAddress.getLocalHost().getHostName() + ":" + port;
            return endpoint;
        }
        catch (UnknownHostException e) {
            return "";
        }
    }

    public PageCacheWarmer getPageCacheWarmer() {
        return this.warmer;
    }
}
