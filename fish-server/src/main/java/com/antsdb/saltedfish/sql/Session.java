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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.InputMismatchException;
import org.antlr.v4.runtime.NoViableAltException;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.slf4j.Logger;

import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.Script;
import com.antsdb.saltedfish.sql.vdm.Transaction;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.util.UberUtil;

public class Session {
    static Logger _log = UberUtil.getThisLogger();
    static AtomicInteger _id = new AtomicInteger();
            
    Orca orca;
    Map<String, Object> variables = new ConcurrentHashMap<String, Object>();
    boolean autoCommit = true;
    String currentNameSpace = "TEST";
    SqlParserFactory fac;
    Map<Integer, PreparedStatement> prepared = new HashMap<Integer, PreparedStatement>();
    String user;
    int resetAutoCommit = 0;
    ConcurrentMap<Integer, TableLock> tableLocks = new ConcurrentHashMap<>();

    private Transaction trx;
	private boolean isClosed = false;
	private long lastInsertId;
	private int id = _id.incrementAndGet();
	private String protocolCharset = "latin1";
	private SessionParameters parameters = new SessionParameters();
    
    public Session(Orca orca, SqlParserFactory fac, String user) {
        this.orca = orca;
        this.fac = fac;
        this.user = user;
    }
    
    public Object run(String sql) throws SQLException {
    	return run(sql, new Parameters());
    }
    
    public Object run(String sql, Parameters params) throws SQLException {
    	return run(new ANTLRInputStream(sql), params);
    }
    
    public Object run(CharStream cs) throws SQLException {
    	return run(cs, new Parameters());
    }
    
    public Object run(CharStream cs, Parameters params) throws SQLException {
    	if (this.isClosed) {
    		throw new OrcaException("session is closed");
    	}
    	
        // main stuff
        
        startTrx();
        Script script = parse(cs);
        VdmContext ctx = new VdmContext(this, script.getVariableCount());
        return script.run(ctx, params, 0);
    }
    
    public Script parse(CharStream cs) {
    	if (this.isClosed) {
    		throw new OrcaException("session is closed");
    	}
    	
        startTrx();
        
        // check the global statement cache
        
        Script script = this.getOrca().getCachedStatement(cs.toString());

        // parse it for real if it is not cached
        
        if (script == null) {
        	try {
        		script = this.fac.parse(this, cs);
        	}
        	catch (ParseCancellationException x) {
        		if (x.getCause() instanceof InputMismatchException) {
        			InputMismatchException xx = (InputMismatchException)x.getCause();
        			String offend = xx.getOffendingToken().getText();
            		throw new OrcaException(x, "Invalid SQL. Offending token: {}", offend);
        		}
        		if (x.getCause() instanceof NoViableAltException) {
        			NoViableAltException xx = (NoViableAltException)x.getCause();
        			String offend = xx.getOffendingToken().getText();
            		throw new OrcaException(x, "Invalid SQL. Offending token: {}", offend);
        		}
        		throw new OrcaException("Invalid SQL", x);
        	}
        	catch (CompileDdlException x) {
        		script = new Script(null, x.nParameters, 100, cs.toString());
        	}
            if (_log.isTraceEnabled()) {
                _log.trace("{}-{}: {}", getId(), script.hashCode(), cs.toString());
            }
            if (script.worthCache()) {
                this.getOrca().cacheStatement(script);
            }
        }
        
        return script;
    }
    
    public Script parse(String text) {
    	return parse(new ANTLRInputStream(text));
    }

    public String getCurrentCatalog() {
        return null;
    }
    
    public Session setCurrentNamespace(String namespace) {
        this.currentNameSpace = namespace;
        return this;
    }
    
    public String getCurrentNamespace() {
        return this.currentNameSpace;
    }
    
    public String getCurrentSchema() {
        return this.currentNameSpace;
    }
    
    public Orca getOrca() {
        return this.orca;
    }
    
    public Map<String, Object> getVariables() {
        return this.variables;
    }
    
    public Object getVariable(String key) {
        return this.variables.get(key);
    }

    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    public boolean isAutoCommit() {
        return this.autoCommit;
    }

    public void commit() {
        endTransaction(true);
    }

    public void rollback() {
    	endTransaction(false);
    }

    public void close() {
        rollback();
        this.orca.sessions.remove(this);
        this.isClosed  = true;
    }
    
    public boolean isClosed() {
    	return this.isClosed;
    }
    
    private void endTransaction(boolean success) {
    	// only if there is a transaction
    	
    	if (this.trx == null) {
    		return;
    	}

    	try {
        	// commit/rollback
        	
    		long trxid = trx.getTrxId(); 
    		long trxts = -1;
    		if (trxid < 0) {
    			if (success) {
    	            trxts = this.orca.getTrxMan().getNewVersion();
    		    	_log.trace("commit trxid={} trxts={}", trxid, trxts);
    				this.orca.getHumpback().commit(trxid, trxts);
    			}
    			else {
    		    	_log.trace("rollback trxid={} trxts={}", trxid);
    				this.orca.getHumpback().rollback(trxid);;
    			}
    		}
    		else {
    			_log.trace("autonomous trxid={}", trxid);
    		}

    		// notify metadata service 
    		
        	if (trx.isDddl()) {
        		this.orca.getMetaService().commit(trx, trxts);
        	}
        	
        	// release all locks
            
            trx.releaseAllLocks();            
            
            // reset auto commit 
            
            if (this.resetAutoCommit > 0) {
            	this.resetAutoCommit--;
            	if (this.resetAutoCommit == 0) {
                	this.autoCommit = true;
            	}
            }
            
            // done
            
            this.trx.reset();
    	}
    	catch (Throwable x) {
    		_log.error("fatal!!!", x);
    		throw x;
    	}
    }
    
    /**
     * start transaction only if it is not started
     */
    public void startTrx() {
    }
    
    /**
     * reset transaction time stamp
     */
    public void resetTrxTs() {
        if (this.trx != null) {
            this.trx.newTrxTs();
        }
    }
    
    public Transaction getTransaction_() {
    	return this.trx;
    }
    
    public Transaction getTransaction() {
        if (this.trx == null) {
            this.trx = new Transaction(this.getOrca().getTrxMan());
        }
        return this.trx;
    }

    public void setParameter(String name, Object value) {
    	this.parameters.setParameter(name, value);
    }

    public int getId() {
        return this.id;
    }

    public PreparedStatement prepare(String sql) throws SQLException {
    	PreparedStatement stmt = new PreparedStatement(this, sql);
        this.prepared.put(stmt.hashCode(), stmt);
        return stmt;
    }
    
    public PreparedStatement getPrepared(int id) {
        return this.prepared.get(id);
    }
    
    public void closePrepared(int id) {
        this.prepared.remove(id);
    }

    public String getUser() {
        return user;
    }

    public SqlParserFactory getParserFactory() {
    	return this.fac;
    }

    public int getLockTimeoutMilliSeconds() {
    	return this.parameters.getLockTimeout();
    }

	public String getProtocolCharset() {
		return this.protocolCharset;
	}
	
	public void setProtocolCharset(String cs) {
		this.protocolCharset = cs;
	}
	
	/**
	 * last insert id is the last value generated for a auto_increment column.
	 * 
	 * @see https://dev.mysql.com/doc/connector-j/5.1/en/connector-j-usagenotes-last-insert-id.html
	 * @param value
	 */
	public void setLastInsertId(long value) {
		this.lastInsertId = value;
	}
	
	public long getLastInsertId() {
		return this.lastInsertId;
	}

	/**
	 * reset auto commit to true after the current transaction. it is required by START TRANSACTION statement
	 */
	public void resetAutoCommitAfterTrx() {
		this.resetAutoCommit = 2;
	}
	
	public void unlockTable(int tableId) {
		unlockTable(getId(), tableId);
	}
	
	public void unlockTable(int owner, int tableId) {
		TableLock lock = this.tableLocks.get(tableId);
		if (lock == null) {
			throw new OrcaException("lock is not found");
		}
		
		// unlock
		
		int level = lock.getLevel();
		lock.release(owner);
		
		// unlock all other session
		
		if (level == LockLevel.EXCLUSIVE) {
			for (Session i:this.orca.sessions) {
				if (i == this) {
					continue;
				}
				i.unlockTable(owner, tableId);
			}
		}
	}
	
	public void lockTable(int owner, int tableId, int level, boolean transactional) {
		// guaranteed getting a table lock
		
		TableLock lock = this.tableLocks.get(tableId);
		if (lock == null) {
			TableLock newone = new TableLock(getId(), tableId);
			lock = tableLocks.putIfAbsent(tableId, newone);
			if (lock == null) {
				lock = newone;
			}
		}
		
		// lock the sucker
		
		if (!lock.acquire(owner, level, getLockTimeoutMilliSeconds())) {
			return;
		}
		
		// is it transactional ?
		
		if (transactional && (!lock.isTransactional())) {
			Transaction trx = getTransaction();
			trx.addLock(lock);
		}
		if (!transactional && lock.isTransactional()) {
			lock.setTransactional(false);
		}
		
		// for exclusive lock, we need to lock out all other session
		
		if (level == LockLevel.EXCLUSIVE) {
			List<Session> undo = new ArrayList<>();
			boolean success = false;
			try {
				for (Session i:this.orca.sessions) {
					if (i == this) {
						continue;
					}
					i.lockTable(owner, tableId, LockLevel.EXCLUSIVE_BY_OTHER, false);
					undo.add(i);
				}
				success = true;
			}
			finally {
				if (!success) {
					for (Session i:undo) {
						i.unlockTable(owner, tableId);
					}
				}
			}
		}
	}
	
	public void lockTable(int tableId, int lockLevel, boolean transactional) {
		lockTable(getId(), tableId, lockLevel, transactional);
	}

	public void unlockAll() {
		for (Map.Entry<Integer, TableLock> i:this.tableLocks.entrySet()) {
			int tableId = i.getKey();
			TableLock lock = i.getValue();
			if (lock.getLevel() == LockLevel.EXCLUSIVE) {
				unlockTable(getId(), tableId);
			}
		}
	}
	
	public SessionParameters getParameters() {
		return this.parameters;
	}
}
