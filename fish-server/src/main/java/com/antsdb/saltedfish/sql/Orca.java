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
package com.antsdb.saltedfish.sql;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;

import com.antsdb.saltedfish.beluga.Pod;
import com.antsdb.saltedfish.beluga.SlaveWarmer;
import com.antsdb.saltedfish.beluga.SystemViewSlaveWarmerInfo;
import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.HumpbackSession;
import com.antsdb.saltedfish.nosql.Replicable;
import com.antsdb.saltedfish.nosql.Replicator;
import com.antsdb.saltedfish.nosql.SpaceManager;
import com.antsdb.saltedfish.nosql.TableType;
import com.antsdb.saltedfish.nosql.TrxMan;
import com.antsdb.saltedfish.server.mysql.replication.MysqlSlave;
import com.antsdb.saltedfish.sql.meta.MetadataService;
import com.antsdb.saltedfish.sql.meta.TableId;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.sql.mysql.MysqlDialect;
import com.antsdb.saltedfish.sql.vdm.ObjectName;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.Script;
import com.antsdb.saltedfish.sql.vdm.Transaction;
import com.antsdb.saltedfish.sql.vdm.Validate;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.sql.vdm.View;
import com.antsdb.saltedfish.storage.HBaseStorageService;
import com.antsdb.saltedfish.storage.SystemViewHBase;
import com.antsdb.saltedfish.util.FishJobManager;
import com.antsdb.saltedfish.util.ManifestUtil;
import com.antsdb.saltedfish.util.UberUtil;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import static com.antsdb.saltedfish.sql.OrcaConstant.*;

/**
 * 
 * distributed sql database engine
 * 
 * @author *-xguo0<@
 *
 */
public class Orca {
    public static String _version = "5.6.0-antsdb";
    public static final String SYSNS = Humpback.SYS_NAMESAPCE;
    
    static Logger _log = UberUtil.getThisLogger();
    static Orca _default;
    static List<Orca> _instances = Collections.synchronizedList(new LinkedList<Orca>());
    static Map<String, SqlDialect> _dialects = new HashMap<>();
    
    Humpback humpback;
    IdentifierService idService;
    MetadataService metaService;
    Map<String, Object> variables = new ConcurrentHashMap<String, Object>();
    Cache<String, Script> statementCache;
    Map<String, View> sysviews = new HashMap<>();
    Map<String, String> namespaces = new HashMap<>();
    volatile boolean isClosed = false;
    Set<Session> sessions = ConcurrentHashMap.newKeySet();
    private ScheduledFuture<?> sessionSweeperFuture;
    private File home;
    private SqlDialect dialect;
    private SessionSweeper sessionSweeper;
    private ScheduledFuture<?> recyclerFuture;
    private Replicator<Replicable> replicator;
    private Session sysdefault;
    SystemParameters config;
    ConcurrentLinkedQueue<DeadSession> deadSessions = new ConcurrentLinkedQueue<>();
    private Pod pod;
    private volatile boolean isSlave;
    
    static {
        Map<String, String> manifest = ManifestUtil.load(Orca.class);
        String version = manifest.get("Implementation-Version");
        version = StringUtils.replace(version, ".", "");
        _version = String.format("5.6.0-antsdb%s", version);
    }
    
    static class DeadSession {
        Session session;
        long timestamp;
        
        DeadSession(Session session) {
            this.session = session;
            this.timestamp = System.currentTimeMillis();
        }
    }
    
    class ShutdownThread extends Thread {
        @Override
        public void run() {
            shutdown();
        }
    }
    
    public Orca(File home, Properties props) throws Exception {
        _log.info("starting orca at {} version {}", home, _version);
        this.home = home;
        _instances.add(this);
        String hbaseConf = props.getProperty("hbase_conf", null);
        this.humpback = new Humpback(home, hbaseConf==null);
        this.humpback.open();
        this.humpback.disableShutdownHook();
        this.metaService = new MetadataService(this);

        // create seed 
        
        init();
        
        // global parameters
        
        this.config = new SystemParameters(this.humpback);
        this.config.set("autocommit", "1");
        this.config.set("auto_increment_increment", "1");
        this.config.set("character_set_client", "utf8");
        this.config.set("character_set_results", "utf8");
        this.config.set("character_set_connection", "utf8");
        this.config.set("collation_database", "utf8_general_ci");
        this.config.set("lower_case_file_system", "NO");
        this.config.set("lower_case_table_names", "1");
        this.config.set("max_allowed_packet", String.valueOf(32 * 1024 * 1024));
        this.config.set("server_id", String.valueOf(getHumpback().getServerId()));
        this.config.set("sql_mode", "");
        this.config.set("tx_isolation", "READ-COMMITTED");
        this.config.set("version", _version);
        this.config.set("version_comment", Orca._version + " Enterprise");

        // init statement cache
        
        this.statementCache = CacheBuilder.newBuilder().maximumSize(1000).build();

        // start services
        
        this.idService = new IdentifierService(this);
        if (UberUtil.between(this.humpback.getServerId(), 0, 1)) {
            this.idService.setMod(2, (int)this.humpback.getServerId());
        }
        
        // skipping validation. it no longer applicable.
        // validate();
        
        // init dialect
        
        _log.info("orca dialect: {}", this.config.getDatabaseType());
        this.dialect = getDialect(this.config.getDatabaseType());
        if (this.dialect == null) {
            throw new OrcaException("unknown dialect {}", this.config.getDatabaseType());
        }
        this.dialect.init(this);
        
        // validate
        
        verifySystem();
        
        // start service threads
        
        new RecycleThread(this).start();

        // system views

        initViews();
        
        // backend jobs
        
        FishJobManager jobman = getJobManager();
        jobman.schedule(10, TimeUnit.SECONDS, ()-> {
            if (this.isClosed) {
                return;
            }
            this.sessionSweeper = new SessionSweeper(this);
            this.sessionSweeperFuture = jobman.scheduleWithFixedDelay(this.sessionSweeper, 2, TimeUnit.SECONDS);
            ResourceRecycler recycler = new ResourceRecycler(this);
            this.recyclerFuture = getJobManager().scheduleWithFixedDelay(recycler, 60, TimeUnit.SECONDS);
        });
        jobman.scheduleWithFixedDelay(60, TimeUnit.SECONDS, ()-> {
            // remove dead sessions 
            long now = System.currentTimeMillis();
            for (DeadSession i:this.deadSessions) {
                if ((now - i.timestamp) > 60 * 1000) {
                    this.deadSessions.remove(i);
                }
            }
        });
        Runtime.getRuntime().addShutdownHook(new ShutdownThread());
        if (this.humpback.getStorageEngine().getReplicable() != null) {
            getHBaseStorageService().setMetaService(this.metaService);
            Replicable replicable = this.humpback.getStorageEngine().getReplicable();
            this.replicator = new Replicator<>("hbase-replicator", this.humpback, replicable, false);
            this.replicator.start();
        }

        // system default session. used as the template for the use sessions
        
        this.sysdefault = createSession("system", "default");
        
        // load cluster
        
        this.pod = new Pod(this);
        this.pod.open();
        this.pod.start();
        this.humpback.addLogDependency(this.pod);
    }
    
    private void initViews() {
        registerSystemView(SYSNS, TABLENAME_SYSSEQUENCE, new SysSequence(this));
        registerSystemView(SYSNS, TABLENAME_SYSTABLE, new SysTable(this));
        registerSystemView(SYSNS, TABLENAME_SYSCOLUMN, new SysColumn(this));
        registerSystemView(SYSNS, TABLENAME_SYSRULE, new SysRule(this));
        registerSystemView(SYSNS, TABLENAME_SYSUSER, new SysUser());

        registerSystemView(SYSNS, "STATEMENT_METRICS", new SystemMetrics(this));
        registerSystemView(SYSNS, "TRX", new SystemViewTrx(this));
        registerSystemView(SYSNS, "HBASE", new SystemViewHBase(this));
        registerSystemView(SYSNS, "SYSTEM_INFO", new SystemInfoView(this));
        registerSystemView(SYSNS, "TABLETS", new SystemViewTablets(this));
        registerSystemView(SYSNS, "SESSIONS", new SystemViewSessions(this));
        registerSystemView(SYSNS, "CONCURRENCY_STATS", new SystemViewConcurrencyStats(this));
        registerSystemView(SYSNS, "LOCKS", new SystemViewLocks(this));
        registerSystemView(SYSNS, "CACHE_INFO", new SystemViewCacheInfo(this));
        registerSystemView(SYSNS, "MINKE_INFO", new SystemViewMinkeInfo(this));
        registerSystemView(SYSNS, "REPLICATOR_INFO", new SystemViewReplicatorInfo());
        registerSystemView(SYSNS, "SLAVE_INFO", new SystemViewSlaveInfo());
        registerSystemView(SYSNS, "SYNCHRONIZER_INFO", new SystemViewSynchronizerInfo());
        registerSystemView(SYSNS, "TABLE_STATS", new SystemViewTableStats(this));
        registerSystemView(SYSNS, "PAGES", new SystemViewPages(this));
        registerSystemView(SYSNS, "x0", new SystemViewHumpbackMeta(this));
        registerSystemView(SYSNS, "x1", new SystemViewHumpbackNamespace(this));
        registerSystemView(SYSNS, "x2", new SystemViewTableStats(this));
        registerSystemView(SYSNS, "x3", new SystemViewHumpbackColumns(this));
        registerSystemView(SYSNS, "x4", new SystemViewHumpbackConfig());
        registerSystemView(SYSNS, "SLOW_QUERIES", new SlowQueries());
        registerSystemView(SYSNS, "CLUSTER_STATUS", new SystemViewClusterStatus());
        registerSystemView(SYSNS, "LOG_DEPENDENCY", new SystemViewLogDependency());
        registerSystemView(SYSNS, "HSESSIONS", new SystemViewHSessions());
        registerSystemView(SYSNS, "MINKE_PAGES", new SystemViewMinkePages());
        registerSystemView(SYSNS, "MEM_IMMORTAL", new SystemViewMemImmortals());
        registerSystemView(SYSNS, "SLAVE_WARMER_INFO", new SystemViewSlaveWarmerInfo());
        registerSystemView(SYSNS, "PAGE_CACHE_WARMER_INFO", new PageCacheWarmerInfo());
    }

    private void verifySystem() {
        if (this.humpback.getNamespace(SYSNS) == null) {
            throw new OrcaException("namespace " + Orca.SYSNS + " is not found");
        }
        if (this.humpback.getTable(TableId.SYSTABLE) == null) {
            throw new OrcaException("systable is not found");
        }
        if (this.humpback.getTable(TableId.SYSSEQUENCE) == null) {
            throw new OrcaException("syssequence is not found");
        }
    }

    void init() throws Exception {
        Seed seed = new Seed(this);
        seed.run();
    }
    
    public HBaseStorageService getHBaseStorageService() {
        return this.humpback.getHBaseService();
    }
    
    public IdentifierService getIdentityService() {
        return this.idService;
    }
    
    public Object getVariable(String name) {
        return this.variables.get(name);
    }

    public MetadataService getMetaService() {
        return metaService;
    }

    void validate() {
        // verify system tables
        
        if (this.humpback.getTable(TABLEID_SYSTABLE) == null) {
            throw new OrcaException("system table not found: " + TABLENAME_SYSTABLE);
        }
        if (this.humpback.getTable(TABLEID_SYSCOLUMN) == null) {
            throw new OrcaException("system table not found: " + TABLENAME_SYSCOLUMN);
        }
        if (this.humpback.getTable(TABLEID_SYSSEQUENCE) == null) {
            throw new OrcaException("system table not found: " + TABLENAME_SYSSEQUENCE);
        }
        if (this.humpback.getTable(TABLEID_SYSRULE) == null) {
            throw new OrcaException("system table not found: " + TABLENAME_SYSRULE);
        }
        
        // verify tables registered in meta data service
        
        Map<Integer, GTable> tableById = new HashMap<>();
        Session session = createSystemSession();
        VdmContext ctx = new VdmContext(session, 0);
        try (HumpbackSession hsession = session.getHSession().open()) {
            for (String ns:getMetaService().getNamespaces()) {
                for (String tableName:getMetaService().getTables(session.getTransaction(), ns)) {
                    ObjectName name = new ObjectName(ns, tableName);
                    TableMeta table = getMetaService().getTable(ctx.getTransaction(), name);
                    GTable gtable = this.humpback.getTable(table.getHtableId());
                    
                    // recreate gtable.it could happen cuz humpback creates the physical when flush the content 
                    
                    if (gtable == null) {
                        gtable = this.humpback.createTable(
                                hsession, 
                                name.getNamespace(), 
                                table.getTableName(), 
                                table.getHtableId(), 
                                TableType.DATA);
                        _log.warn("table {} not found in humpback, created ", name);
                    }
                    else {
                        Validate validator = new Validate(name);
                        try {
                            validator.run(ctx, null);
                        }
                        catch (Exception x) {
                            _log.warn("failed to verify table: " + tableName, x);
                        }
                    }
                    tableById.put(table.getId(), gtable);
                }
            }
            
            // verify tables from humpback
            
            for (GTable i:this.humpback.getTables()) {
                String ns = i.getNamespace();
                int tableId = i.getId();
                if (tableById.get(tableId) == null) {
                    _log.warn("table exists but not registered: " + i.getId());
                    humpback.dropTable(hsession, ns, tableId);
                }
            }
        }
    }
    
    public Session createSession(String user, String remoteEndpoint) {
        if (this.isClosed) {
            throw new OrcaException("orca is closed");
        }
        Session session = new Session(this, getSqlParserFactory(), user, remoteEndpoint);
        if (this.sysdefault != null) {
            for (Map.Entry<Integer, TableLock> i:this.sysdefault.tableLocks.entrySet()) {
                int tableId = i.getKey();
                TableLock lock = i.getValue();
                TableLock newlock = lock.clone(session.getId());
                session.tableLocks.put(tableId, newlock);
            }
        }
        String msg = String.format("session %d user %s from %s", session.getId(), user, remoteEndpoint);
        getHumpback().getGobbler().logMessage(session.getHSession(), msg);
        this.sessions.add(session);
        return session;
    }
    
    public void closeSession(Session session) {
        if (session.isClosed()) {
            return;
        }
        session.close();
        this.sessions.remove(session);
        String msg = String.format("session %d is closed", session.getId());
        getHumpback().getGobbler().logMessage(session.getHSession(), msg);
        this.deadSessions.add(new DeadSession(session));
        while (this.deadSessions.size() > 200) {
            this.deadSessions.poll();
        }
    }
    
    public Session createSystemSession() {
        Session session = new Session(this, getSqlParserFactory(), "__system", "local");
        session.startTrx();
        return session;
    }
    
    public synchronized void shutdown() {
        if (this.isClosed) {
            return;
        }
        this.isClosed = true;

        // close jobs

        if (this.replicator != null) {
            this.replicator.close();
        }
        if (this.sessionSweeperFuture != null) {
            this.sessionSweeperFuture.cancel(true);
        }
        if (this.recyclerFuture != null) {
            this.recyclerFuture.cancel(true);
        }

        // close slave thread

        try {
            MysqlSlave.stopIfExists();
        }
        catch (Exception x) {
            _log.error("unable to stop slave threads", x);
        }

        // close cluster threads;

        try {
            this.pod.close();
        }
        catch (Exception x) {
            _log.error("unable to stop cluster threads", x);
        }
        
        // close all sessions

        if (this.sessionSweeperFuture != null) {
            this.sessionSweeperFuture.cancel(false);
        }
        if (this.recyclerFuture != null) {
            this.recyclerFuture.cancel(false);
        }
        for (Session i:this.sessions) {
            closeSession(i);
        }

        // close services

        this.idService.close();
        this.metaService.close();
        
        // shutdown hbase storage service

        try {
            if (this.replicator != null) {
                this.replicator.close();
            }
            if (getHBaseStorageService() != null) {
                getHBaseStorageService().shutdown();
            }
        }
        catch(Exception e) {
        }

        // shutdown humpback

        humpback.shutdown();

    }
    
    public Humpback getHumpback() {
        return this.humpback;
    }
    
    SqlParserFactory getSqlParserFactory() {
        return this.dialect.getParserFactory();
    }
    
    public View getSystemView(String ns, String table) {
        String key = (ns + '.' + table).toLowerCase();
        return this.sysviews.get(key);
    }
    
    public void registerSystemView(String ns, String tableName, View maker) {
        // register namespace if absent
        
        if (this.namespaces.get(ns.toLowerCase()) == null) {
            this.namespaces.put(ns.toLowerCase(), ns);
        }
        
        // register view
        
        String key = (ns + '.' + tableName).toLowerCase();
        maker.setName(new ObjectName(ns, tableName));
        this.sysviews.put(key, maker);
    }
    
    public Script getCachedStatement(String text) {
        Script script = this.statementCache.asMap().get(text);
        return script;
    }

    public void cacheStatement(Script script) {
        this.statementCache.put(script.getSql(), script);
    }

    public void clearStatementCache() {
        this.statementCache.invalidateAll();
    }
    
    public String getExternalNamespace(String ns) {
        return this.namespaces.get(ns.toLowerCase());
    }

    public SystemParameters getConfig() {
        return this.config;
    }

    public final SpaceManager getSpaceManager() {
        return this.humpback.getSpaceManager();
    }

    public TrxMan getTrxMan() {
        return this.humpback.getTrxMan();
    }

    public DataTypeFactory getTypeFactory() {
        return this.dialect.getTypeFactory();
    }
    
    public long getNextRowid() {
        return this.idService.getNextRowid();
    }

    public File getHome() {
        return this.home;
    }
    
    public FishJobManager getJobManager() {
        return this.humpback.getJobManager();
    }

    public SqlDialect getDialect() {
        return this.dialect;
    }
    
    public static void registerDialect(SqlDialect dialect) {
        _dialects.put(dialect.getName(), dialect);
    }

    public static SqlDialect getDialect(String name) {
        SqlDialect dialect = _dialects.get(name);
        if (dialect == null) {
            dialect = new MysqlDialect();
        }
        return dialect;
    }
    
    public SessionSweeper getSessionSweeper() {
        return this.sessionSweeper;
    }
    
    /**
     * get the start sp of any active or future transaction
     * 
     * @return never 0
     */
    public long getStartSp() {
        long result = this.humpback.getSpaceManager().getAllocationPointer();
        for (Session session:this.sessions) {
            long sp = session.getStartSp();
            if (sp == 0) {
                continue;
            }
            result = Math.min(result, sp);
        }
        return result;
    }

    /**
     * free unused resource
     */
    public void recycle() {
        this.humpback.recycle();
    }
    
    /**
     * get the oldest trx id from all sessions
     * 
     * @return 0 if there is no active trx
     */
    public long getOldestTrxId() {
        long oldest = Long.MIN_VALUE;
        for (Session session:this.sessions) {
            Transaction trx = session.getTransaction_();
            if (trx == null) {
                continue;
            }
            long trxid = trx.getTrxId();
            if (trxid == 0) {
                continue;
            }
            oldest = Math.max(trxid, oldest);
        }
        return (oldest == Long.MIN_VALUE) ? 0 : oldest;
    }
    
    public long getLastClosedTransactionId() {
        long currentTrxId = getTrxMan().getLastTrxId();
        UberUtil.sleep(200);
        long oldest = getOldestTrxId();
        if (oldest == 0) {
            // no active trx, get last of the current trxid
            return currentTrxId;
        }
        else {
            return Math.max(currentTrxId, oldest+1);
        }
    }
    
    public Set<Session> getSessions() {
        return this.sessions;
    }
    
    public Session getSession(int sessionId) {
        for (Session i:this.sessions) {
            if (i.getId() == sessionId) {
                return i;
            }
        }
        return null;
    }
    
    public Replicator<Replicable> getReplicator() {
        return this.replicator;
    }
    
    public Session getDefaultSession() {
        return this.sysdefault;
    }
    
    public AuthPlugin getAuthPlugin() {
        String name = this.config.getAuthPlugin();
        if ("mysql_native_password".equals(name)) {
            return new NativeAuthPlugin(this);
        }
        else {
            return new NoPasswordAuthPlugin(this);
        }
    }
    public Pod getBelugaPod() {
        return this.pod;
    }

    public void setSlaveMode(boolean value) {
        this.isSlave = value;
    }
    
    public boolean isSlaveMode() {
        return this.isSlave;
    }

    public boolean isLeader() {
        if (!this.pod.isInCluster()) {
            return true;
        }
        return this.pod.isLeader();
    }

    public boolean isClosed() {
        return this.isClosed;
    }

    public void sendToSlaveWarmer(String ns, String sql, Parameters params, Object result) {
        SlaveWarmer warmer = this.pod.getWarmer();
        if (warmer != null) {
            warmer.send(ns, sql, params, result);
        }
    }
}
