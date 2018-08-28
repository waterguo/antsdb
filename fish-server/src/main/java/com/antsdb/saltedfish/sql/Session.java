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

import java.nio.CharBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.InputMismatchException;
import org.antlr.v4.runtime.NoViableAltException;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.slf4j.Logger;

import com.antsdb.saltedfish.nosql.HumpbackSession;
import com.antsdb.saltedfish.sql.command.FishParserFactory;
import com.antsdb.saltedfish.sql.vdm.AsynchronousInsert;
import com.antsdb.saltedfish.sql.vdm.Cursor;
import com.antsdb.saltedfish.sql.vdm.FishParameters;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.Script;
import com.antsdb.saltedfish.sql.vdm.Transaction;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.util.Callback;
import com.antsdb.saltedfish.util.LatencyDetector;
import com.antsdb.saltedfish.util.UberTime;
import com.antsdb.saltedfish.util.UberUtil;

public class Session {
    static Logger _log = UberUtil.getThisLogger();
    static AtomicInteger _id = new AtomicInteger();
            
    Orca orca;
    Map<String, Object> variables = new ConcurrentHashMap<String, Object>();
    String currentNameSpace = "TEST";
    FishParserFactory fishParser = new FishParserFactory();
    SqlParserFactory fac;
    Map<Integer, PreparedStatement> prepared = new HashMap<Integer, PreparedStatement>();
    String user;
    int resetAutoCommit = 0;
    ConcurrentMap<Integer, TableLock> tableLocks = new ConcurrentHashMap<>();

    private volatile Transaction trx;
    private AtomicBoolean isClosed = new AtomicBoolean(false);
    private long lastInsertId;
    private int id = _id.incrementAndGet();
    AsynchronousInsert asyncExecutor;
    private HumpbackSession hsession;
    public String remote;
    private SystemParameters config;
    private volatile Thread thread;
    private Object sql;
    ConcurrentSkipListMap<Long, Execution> slowOnes = new ConcurrentSkipListMap<>();
    
    public Session(Orca orca, SqlParserFactory fac, String user) {
        this.orca = orca;
        this.fac = fac;
        this.user = user;
        this.hsession = orca.getHumpback().createSession();
        this.config = new SystemParameters(orca.config);
    }
    
    public Object run(String sql) throws SQLException {
        return run(sql, new Parameters());
    }
    
    public Object run(String sql, Parameters params) throws SQLException {
        return run(CharBuffer.wrap(sql), params, null);
    }
    
    public Object run(String sql, Parameters params, Consumer<Object> callback) throws SQLException {
        return run(CharBuffer.wrap(sql), params, callback);
    }
    
    public Object run(CharBuffer cbuf, Parameters params, Consumer<Object> callback) throws SQLException {
        long startTime = UberTime.getTime();
        try {
            this.sql = cbuf;
            this.thread = Thread.currentThread();
            if (this.isClosed.get()) {
                throw new OrcaException("session is closed");
            }
                
            Object result = null;
            if ((this.asyncExecutor != null) && isInsert(cbuf)) {
                // import 
                if (getTransaction().getTrxId() == 0) {
                    this.trx.getGuaranteedTrxId();
                }
                result = asyncInsert(cbuf);
            }
            else {
                // main stuff
                startTrx();
                Script script = LatencyDetector.run(_log, "parse", ()->{
                    return parse0(cbuf);
                });
                VdmContext ctx = new VdmContext(this, script.getVariableCount());
                result = script.run(ctx, params, 0);
                if (result instanceof Cursor) {
                    result = new FinalCursor(script, params, (Cursor)result);
                }
            }
            
            // write result back
            
            if (callback != null) {
                callback.accept(result);
            }
            return result;
        }
        finally {
            this.sql = null;
            this.thread = null;
            long time = UberTime.getTime() - startTime;
            trackQueryTime(cbuf, null, time);
        }
    }
    
    public Object run(PreparedStatement stmt, Parameters params, Consumer<Object> consumer) {
        long startTime = UberTime.getTime();
        try {
            this.thread = Thread.currentThread();
            if (this.isClosed.get()) {
                throw new OrcaException("session is closed");
            }
            this.sql = stmt.sql;
            Object result = stmt.run(this, params);
            if (result instanceof Cursor) {
                result = new FinalCursor(stmt.script, params, (Cursor)result);
            }
            stmt.result = result;
            if (consumer != null) {
                consumer.accept(result);
            }
            return result;
        }
        finally {
            this.thread = null;
            this.sql = null;
            long time = UberTime.getTime() - startTime;
            trackQueryTime(stmt.sql, params, time);
        }
    }
    
    private Integer asyncInsert(CharBuffer cs) {
        if (this.asyncExecutor.getError() != null) {
            throw new OrcaException(this.asyncExecutor.getError());
        }
        this.asyncExecutor.add(cs);
        return 1;
    }

    private boolean isInsert(CharBuffer cs) {
        return beginWith(cs, "INSERT INTO ");
    }

    private boolean beginWith(CharBuffer cs, String s) {
        int idx = cs.position();
        for (int i=0; i<s.length(); i++) {
            if (cs.get(idx + i) != s.charAt(i)) {
                return false;
            }
        }
        return true;
    }

    public Script parse(CharBuffer cbuf) {
        try {
            this.thread = Thread.currentThread();
            return parse0(cbuf);
        }
        finally {
            this.thread = null;
        }
    }
    
    private Script parse0(CharBuffer cbuf) {
        if (this.isClosed.get()) {
            throw new OrcaException("session is closed");
        }
    
        CharStream cs = new CharBufferStream(cbuf);
        startTrx();
        
        // check the global statement cache
        
        Script script = this.getOrca().getCachedStatement(cs.toString());

        // parse it for real if it is not cached
        
        if (script == null) {
            try {
                if (cs.LA(1) == '.') {
                    cs.consume();
                    script = this.fishParser.parse(this, cs);
                }
                else {
                    script = this.fac.parse(this, cs);
                }
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
        return parse(CharBuffer.wrap(text));
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
    
    public void setVariable(String name, Object val) {
        if (val == null) {
            this.variables.remove(name);
        }
        else {
            this.variables.put(name, val);
        }
    }
    
    public Object getVariable(String key) {
        return this.variables.get(key);
    }

    public Map<String, Object> getVariables() {
        return this.variables;
    }
    
    public void setAutoCommit(boolean autoCommit) {
        this.config.set("autocommit", autoCommit ? "1" : "0");
    }

    public boolean isAutoCommit() {
        return this.config.getAutoCommit();
    }

    public void commit() {
        endTransaction(true);
    }

    public void rollback() {
        endTransaction(false);
    }

    public void close() {
        for (;;) {
            // already closed return
            if (this.isClosed.get()) {
                return;
            }
            // if not, go to the close process
            if (this.isClosed.compareAndSet(false, true)) {
                break;
            }
        }
        if (this.thread != null) {
            // wait 1 second for the session to end
            
            UberUtil.sleep(1000);
            
            // otherwise, force the thread to stop
            
            if (this.thread != null) {
                this.thread.interrupt();
                UberUtil.sleep(3000);
            }
        }
        rollback();
        unlockAll();
        this.orca.sessions.remove(this);
    }
    
    public boolean isClosed() {
        return this.isClosed.get();
    }
    
    private void endTransaction(boolean success) {
        waitForAsyncExecution();
        
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
                        this.orca.getHumpback().commit(trxid, trxts, this.id);
                    }
                    else {
                        _log.trace("rollback trxid={} trxts={}", trxid);
                        this.orca.getHumpback().rollback(trxid, this.id);;
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
                        setAutoCommit(true);
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

    public int getId() {
        return this.id;
    }

    public PreparedStatement prepare(String sql) throws SQLException {
        return prepare(CharBuffer.wrap(sql));
    }
    
    public PreparedStatement prepare(CharBuffer sql) throws SQLException {
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
    
    public boolean lockTable(int owner, int tableId, int level, boolean transactional) {
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
        
        if (!lock.acquire(owner, level, this.config.getLockTimeout())) {
            return false;
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
        return true;
    }
    
    public boolean lockTable(int tableId, int lockLevel, boolean transactional) {
        return lockTable(getId(), tableId, lockLevel, transactional);
    }

    public void unlockAll() {
        List<Integer> tables = new ArrayList<>();
        for (Map.Entry<Integer, TableLock> i:this.tableLocks.entrySet()) {
            int tableId = i.getKey();
            TableLock lock = i.getValue();
            if (lock.getLevel() == LockLevel.EXCLUSIVE) {
                tables.add(tableId);
            }
        }
        for (Integer i:tables) {
            unlockTable(getId(), i);
        }
    }
    
    public SystemParameters getConfig() {
        return this.config;
    }
    
    public void notifyStartQuery() {
        this.hsession.open();
    }
    
    public void notifyEndQuery() {
        this.hsession.close();
    }
    
    /**
     * get start sp if the session is currently in a transaction
     * 
     * @return 0 if there is no active transaction
     */
    public long getStartSp() {
       Transaction result = this.trx;
       return (result != null) ? result.getStartSp() : 0;
    }

    public boolean isImportModeOn() {
        return this.asyncExecutor != null;
    }

    private void waitForAsyncExecution() {
        if (this.asyncExecutor != null) {
            this.asyncExecutor.waitForCompletion();
        }
    }
    
    public void setImportMode(boolean value) {
        if (value) {
            if (this.asyncExecutor != null) {
                return;
            }
            // make all asynchronous in one transaction. otherwise session sweeper cant determine the transaction
            // window correctly
            this.variables.put("##oldAutoCommit", this.config.getAutoCommit());
            setAutoCommit(false);
            this.asyncExecutor = new AsynchronousInsert(this);
        }
        else {
            if (this.asyncExecutor != null) {
                this.asyncExecutor.close();
                commit();
                this.asyncExecutor = null;
                setAutoCommit((Boolean)this.variables.get("##oldAutoCommit"));
            }
        }
    }

    public boolean kill() {
        Thread t = this.thread;
        if (t == null) {
            return false;
        }
        t.interrupt();
        AsynchronousInsert insert = this.asyncExecutor;
        if (insert != null) {
            insert.close();
        }
        return true;
    }

    public String getSql() {
        Object result = this.sql;
        return (result != null) ? result.toString() : null;
    }

    public void fetch(FinalCursor result, Callback callback) {
        try {
            this.thread = Thread.currentThread();
            this.sql = result.script.getSql();
            callback.callback();
        }
        finally {
            this.thread = null;
            this.sql = null;
        }
    }
    
    private void trackQueryTime(CharBuffer sql, Parameters params, long time) {
        Map.Entry<Long, Execution> first = null;
        if (this.slowOnes.size() > 10) {
            first = this.slowOnes.firstEntry();
            if (time <= first.getValue().duration) {
                return;
            }
        }
        Execution result = new Execution();
        result.sql = sql;
        if (params instanceof FishParameters) {
            // FishParameters uses direct memory , it is not session safe
            result.params = ((FishParameters)params).toParameters();
        }
        else {
            result.params = params;
        }
        result.timestamp = UberTime.getTime();
        result.duration = time;
        long key = (time << 32) | result.hashCode();
        this.slowOnes.put(key, result);
        if (first != null) {
            this.slowOnes.remove(first.getKey());
        }
    }

    public long getQueryStartTime() {
        return this.hsession.getOpenTime();
    }
}
