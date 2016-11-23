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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;

import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.SpaceManager;
import com.antsdb.saltedfish.nosql.TableType;
import com.antsdb.saltedfish.nosql.TrxMan;
import com.antsdb.saltedfish.server.mysql.replication.MysqlSlave;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.MetadataService;
import com.antsdb.saltedfish.sql.meta.SequenceMeta;
import com.antsdb.saltedfish.sql.meta.TableId;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.sql.mysql.MysqlDialect;
import com.antsdb.saltedfish.sql.vdm.CursorMaker;
import com.antsdb.saltedfish.sql.vdm.ObjectName;
import com.antsdb.saltedfish.sql.vdm.Script;
import com.antsdb.saltedfish.sql.vdm.Transaction;
import com.antsdb.saltedfish.sql.vdm.Validate;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.storage.HBaseStorageService;
import com.antsdb.saltedfish.storage.SystemViewHBase;
import com.antsdb.saltedfish.util.FishJobManager;
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
	public static final String VERSION = "5.5.0-antsdb1606";
    public static final String SYSNS = "__SYS";
    
    static Logger _log = UberUtil.getThisLogger();
    static Orca _default;
    static List<Orca> _instances = Collections.synchronizedList(new LinkedList<Orca>());
    static Map<String, SqlDialect> _dialects = new HashMap<>();
    
    Humpback humpback;
    IdentifierService idService;
    MetadataService metaService;
    ConfigService config;
    ClusterService clusterService;
    Map<String, Object> variables = new ConcurrentHashMap<String, Object>();
    Cache<String, Script> statementCache;
    Map<String, ExternalTable> externals = new HashMap<>();
    Map<String, CursorMaker> sysviews = new HashMap<>();
    Map<String, String> namespaces = new HashMap<>();
    boolean isClosed = false;
    Set<Session> sessions = ConcurrentHashMap.newKeySet();
    HBaseStorageService hbaseStorageService = null;
	private ScheduledFuture<?> sessionSweeper;
	private File home;
	private SqlDialect dialect; 
    
    class ShutdownThread extends Thread {
        @Override
        public void run() {
            shutdown();
        }
    }
    
    public Orca(File home, Properties props) throws Exception {
        _log.info("starting orca at {}", home);
        this.home = home;
        _instances.add(this);
        String hbaseConf = props.getProperty("hbase_conf", null);
        this.humpback = Humpback.open(home, hbaseConf==null);
        this.humpback.disableShutdownHook();
        this.idService = new IdentifierService(this);
        this.metaService = new MetadataService(this);
        this.config = new ConfigService(this, props);
        
        // init statement cache
        
        this.statementCache = CacheBuilder.newBuilder().maximumSize(1000).build();

        //
        
        init();
        
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
        
        registerSystemView(SYSNS, "STATEMENT_METRICS", new SystemMetrics(this));
        registerSystemView(SYSNS, "TRX", new SystemViewTrx(this));
        registerSystemView(SYSNS, "HBASE", new SystemViewHBase(this));
        registerSystemView(SYSNS, "VALUE", new SystemViewValue(this));
        registerSystemView(SYSNS, "TABLETS", new SystemViewTablets(this));
        registerSystemView(SYSNS, "SESSIONS", new SystemViewSessions(this));
        registerSystemView(SYSNS, "CONCURRENCY_STATS", new SystemViewConcurrencyStats(this));
        registerSystemView(SYSNS, "LOCKS", new SystemViewLocks(this));
        
        // cluster service
        
        this.clusterService = new ClusterService(this.config);
        
        // backend jobs
        
        this.sessionSweeper = getJobManager().scheduleWithFixedDelay(new SessionSweeper(this), 2, TimeUnit.SECONDS);

        Runtime.getRuntime().addShutdownHook(new ShutdownThread());
        
        // start hbase replication
        
        this.hbaseStorageService = this.humpback.getHBaseService();
        if (this.hbaseStorageService != null) {
        	this.hbaseStorageService.setMetaService(this.metaService);
        	this.hbaseStorageService.start();
        }
    }
    
    private void verifySystem() {
    	if (this.humpback.getNamespace(SYSNS) == null) {
    		throw new OrcaException("namespace __sys is not found");
    	}
    	if (this.humpback.getTable(-TableId.SYSTABLE.ordinal()) == null) {
    		throw new OrcaException("systable is not found");
    	}
    	if (this.humpback.getTable(-TableId.SYSSEQUENCE.ordinal()) == null) {
    		throw new OrcaException("syssequence is not found");
    	}
    	Transaction trx = Transaction.getSeeEverythingTrx();
    	if (this.metaService.getSequence(trx, SequenceMeta.SEQ_NAME) == null) {
    		throw new OrcaException("sequence sequenceId is not found");
    	}
	}

	void init() throws Exception {
        if (!isOrcaEnabled()) {
            String dbtype = this.config.getDefaultDatabaseType();
            this.dialect = getDialect(dbtype);
            
            _log.info("database is not found. creating seed database with type {}", dbtype);
            
            // system namespaces
            
            this.humpback.createNamespace(SYSNS);
            
            // system tables
            
            this.humpback.createTable(
            		SYSNS, 
            		String.format("%08x", TABLEID_SYSSEQUENCE), 
            		TABLEID_SYSSEQUENCE, 
            		TableType.DATA);
            this.humpback.createTable(
            		SYSNS, 
            		String.format("%08x", TABLEID_SYSTABLE), 
            		TABLEID_SYSTABLE, 
            		TableType.DATA);
            this.humpback.createTable(
            		SYSNS, 
            		String.format("%08x", TABLEID_SYSCOLUMN), 
            		TABLEID_SYSCOLUMN, 
            		TableType.DATA);
            
            // system sequence 
            
            createSystemSequence(SequenceMeta.SEQ_NAME, 0, 10);
            createSystemSequence(TableMeta.SEQ_NAME, 1, 0);
            createSystemSequence(ColumnMeta.SEQ_NAME, 2, 0);
            createSystemSequence(new ObjectName(SYSNS, TABLENAME_SYSRULE), 3, 0);
            createSystemSequence(new ObjectName(SYSNS, TABLENAME_SYSRULECOL), 4, 0);

            // script
            
            createSystemSession().run(IOUtils.toString(getClass().getResource("init.sql")));
            _log.info("database is created");
            
            // setting up initial parameters
            
            this.config.set("databaseType", dbtype);            
        }
    }
    
    void createSystemSequence(ObjectName name, int id, int next) {
        SequenceMeta seq = new SequenceMeta(name, id);
        seq.setLastNumber(next);
        getMetaService().addSequence(Transaction.getSystemTransaction(), seq);
    }
    
    boolean isOrcaEnabled() {
        return this.humpback.getNamespace(SYSNS) != null;
    }
    
    public Humpback getStroageEngine() {
        return this.humpback;
    }
    
    public HBaseStorageService getHBaseStorageService() {
    	return this.hbaseStorageService;
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
        if (this.humpback.getTable(TABLEID_SYSPARAM) == null) {
            throw new OrcaException("system table not found: " + TABLENAME_SYSPARAM);
        }
        if (this.humpback.getTable(TABLEID_SYSSEQUENCE) == null) {
            throw new OrcaException("system table not found: " + TABLENAME_SYSSEQUENCE);
        }
        if (this.humpback.getTable(TABLEID_SYSRULE) == null) {
            throw new OrcaException("system table not found: " + TABLENAME_SYSRULE);
        }
        if (this.humpback.getTable(TABLEID_SYSRULECOL) == null) {
            throw new OrcaException("system table not found: " + TABLENAME_SYSRULECOL);
        }
        
        // verify tables registered in meta data service
        
        Map<Integer, GTable> tableById = new HashMap<>();
        Session session = createSystemSession();
        VdmContext ctx = new VdmContext(session, 0);
        for (String ns:getMetaService().getNamespaces()) {
            for (String tableName:getMetaService().getTables(session.getTransaction(), ns)) {
                ObjectName name = new ObjectName(ns, tableName);
                TableMeta table = getMetaService().getTable(ctx.getTransaction(), name);
                GTable gtable = this.humpback.getTable(table.getId());
                
                // recreate gtable.it could happen cuz humpback creates the physical when flush the content 
                
                if (gtable == null) {
                    gtable = this.humpback.createTable(
                    		name.getNamespace(), 
                    		table.getTableName(), 
                    		table.getId(), 
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
                humpback.dropTable(ns, tableId);
            }
        }
    }
    
    public Session createSession(String user) {
        Session session = new Session(this, getSqlParserFactory(), user);
        this.sessions.add(session);
        return session;
    }
    
    public void closeSession(Session session) {
    	session.close();
    	this.sessions.remove(session);
    }
    
    public Session createSystemSession() {
        Session session = new Session(this, getSqlParserFactory(), "__system");
        session.startTrx();
        return session;
    }
    
    public synchronized void shutdown() {
        if (this.isClosed) {
            return;
        }
        this.isClosed = true;
        
        // close slave thread
        
        MysqlSlave.stopIfExists();
        
        // close all sessions
        
        this.sessionSweeper.cancel(false);
        for (Session i:this.sessions) {
        	closeSession(i);
        }
        
        // shutdown hbase storage service
        
        try {
        	if (this.hbaseStorageService != null) {
        		this.hbaseStorageService.shutdown();
        		this.hbaseStorageService = null;
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
    
	public Object getSystemView(String ns, String table) {
        String key = (ns + '.' + table).toLowerCase();
        return this.sysviews.get(key);
	}
	
    public void registerSystemView(String ns, String tableName, CursorMaker maker) {
        // register namespace if absent
        
        if (this.namespaces.get(ns.toLowerCase()) == null) {
            this.namespaces.put(ns.toLowerCase(), ns);
        }
        
        // register view
        
        String key = (ns + '.' + tableName).toLowerCase();
        this.sysviews.put(key, maker);
    }
    
    public void registerExternalTable(String ns, String tableName, ExternalTable table) {
        // register namespace if absent
        
        if (this.namespaces.get(ns.toLowerCase()) == null) {
            this.namespaces.put(ns.toLowerCase(), ns);
        }
        
        // register table
        
        String key = (ns + '.' + tableName).toLowerCase();
        // table id for externals starts at -0x1000 and goes down
        int tableId = -0x1000 - this.externals.size();
        table.getMeta().setId(tableId);
        this.externals.put(key, table);
    }
    
    public ExternalTable getExternalTable(String ns, String tableName) {
        String key = (ns + '.' + tableName).toLowerCase();
        return this.externals.get(key);
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

    public ClusterService getClusterService() {
        return this.clusterService;
    }

    public ConfigService getConfigService() {
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
		return this.humpback.getCheckPoint().getAndIncrementRowid();
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
}
