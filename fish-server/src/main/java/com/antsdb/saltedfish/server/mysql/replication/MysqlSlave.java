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
package com.antsdb.saltedfish.server.mysql.replication;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.codec.Charsets;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;

import com.antsdb.saltedfish.nosql.CheckPoint;
import com.antsdb.saltedfish.server.SaltedFish;
import com.antsdb.saltedfish.server.mysql.PreparedStmtHandler;
import com.antsdb.saltedfish.sql.ConfigService;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.PreparedStatement;
import com.antsdb.saltedfish.sql.Session;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.MetadataService;
import com.antsdb.saltedfish.sql.meta.PrimaryKeyMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.Transaction;
import com.antsdb.saltedfish.util.CodingError;
import com.antsdb.saltedfish.util.UberUtil;
import com.google.code.or.OpenReplicator;
import com.google.code.or.binlog.BinlogEventListener;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.DeleteRowsEvent;
import com.google.code.or.binlog.impl.event.DeleteRowsEventV2;
import com.google.code.or.binlog.impl.event.QueryEvent;
import com.google.code.or.binlog.impl.event.RotateEvent;
import com.google.code.or.binlog.impl.event.TableMapEvent;
import com.google.code.or.binlog.impl.event.UpdateRowsEvent;
import com.google.code.or.binlog.impl.event.UpdateRowsEventV2;
import com.google.code.or.binlog.impl.event.WriteRowsEvent;
import com.google.code.or.binlog.impl.event.WriteRowsEventV2;
import com.google.code.or.binlog.impl.event.XidEvent;
import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Pair;
import com.google.code.or.common.glossary.Row;
import com.google.code.or.common.glossary.column.StringColumn;

/**
 * 
 * @author rluo
 */
public class MysqlSlave implements BinlogEventListener {
	static Logger _log = UberUtil.getThisLogger();

	static SaltedFish fish = SaltedFish.getInstance();
    private static MysqlSlave _client = null;
	public static long _trxCounter = 0;
	public static long _insertCount = 0;
	public static long _updateCount = 0;
	public static long _deleteCount = 0;
	
	Session session;
	OpenReplicator or = new OpenReplicator();
    public String masterHost ;
    public int masterPort;
    public String masterUser ;
    public String masterPassword ;
    public String masterBinlog ;
    public long masterLogPos;
    public int serverId;
    
    // String[] should have 2 values in array, first for schema, second for table
    HashMap<Long, String[]> tableById = new HashMap<Long, String[]>();

    PreparedStmtHandler preparedStmtHandler;
    Map<Long, Integer> insertPstmtMap = new HashMap<>();
    Map<Long, Integer> deletePstmtMap = new HashMap<>();
    Map<Long, Integer> updatePstmtMap = new HashMap<>();
	
	private MysqlSlave() {
		session = fish.getOrca().createSession(masterUser);
		serverId = getConfig().getSlaveServerId();
        masterHost = SaltedFish.getInstance().getConfig().getProperties().getProperty("masterHost");
        masterPort = Integer.parseInt(SaltedFish.getInstance().getConfig().getProperties().getProperty("masterPort"));
        masterUser = getConfig().getSlaveUser();
        masterPassword = getConfig().getSlavePassword();
        masterBinlog = getCheckPoint().getSlaveLogFile();
        masterLogPos = getCheckPoint().getSlaveLogPosition();

        or.setUser(masterUser);
		or.setPassword(masterPassword);
		or.setHost(masterHost);
		or.setPort(masterPort);
		or.setServerId(serverId);
		or.setBinlogPosition(masterLogPos);
		or.setBinlogFileName(masterBinlog);
		or.setBinlogEventListener(this);
	}
	
	private ConfigService getConfig() {
		return fish.getOrca().getConfigService();
	}

	CheckPoint getCheckPoint() {
		return fish.getOrca().getHumpback().getCheckPoint();
	}

	// start slave, sync with stop to prevent multi replication
    public static synchronized void start()
    {
    	if (_client != null) {
    		throw new OrcaException("replicator is already running");
    	}
    	_client = new MysqlSlave();
        try {
        	_log.info(
				"starting replicator @ {}:{}", 
				_client.getCheckPoint().getSlaveLogFile(), 
				_client.getCheckPoint().getSlaveLogPosition());
        	_client.or.start();
        }
        catch (Exception x) {
        	_log.error("replication failed", x);
        	throw new CodingError("Failed to start replication");
        }
    }

    // stop slave
    public static synchronized void stop()
    {
    	if (_client == null) {
    		throw new OrcaException("replicator is not running");
    	}
        try {
        	_log.info("stopping replicator ..."); 
			_client.or.stop(5000, TimeUnit.MILLISECONDS);
			_client.session.close();
			_client.session = null;
        }
        catch (Exception x) {
        	throw new OrcaException("Failed to stop replication", x);
        }
        finally {
        	_log.info(
				"replicator is stopped at @ {}:{}", 
				_client.getCheckPoint().getSlaveLogFile(), 
				_client.getCheckPoint().getSlaveLogPosition());
			_client = null;
        }
    }

    public static void stopIfExists() {
    	if (_client != null) {
    		stop();
    	}
	}


	@Override
	public void onEvents(BinlogEventV4 event) {
    	if (_log.isTraceEnabled()) {
    		_log.trace(event.toString());
    	}
        try {
	        if (event instanceof XidEvent) {
	        	onCommit((XidEvent)event);
	        }
	        else if (event instanceof QueryEvent) {
	        	onQuery((QueryEvent)event);
	        	getCheckPoint().setSlaveLogPosition(event.getHeader().getNextPosition());
	        }
	        else if (event instanceof RotateEvent) {
	        	onRotate((RotateEvent)event);
	        }
	        else if (event instanceof WriteRowsEvent) {
	        	onWriteRows((WriteRowsEvent)event);
	        	getCheckPoint().setSlaveLogPosition(event.getHeader().getNextPosition());
	        }
	        else if (event instanceof WriteRowsEventV2) {
	        	onWriteRows((WriteRowsEventV2)event);
	        	getCheckPoint().setSlaveLogPosition(event.getHeader().getNextPosition());
	        }
	        else if (event instanceof UpdateRowsEvent) {
	        	onUpdateRows((UpdateRowsEvent)event);
	        	getCheckPoint().setSlaveLogPosition(event.getHeader().getNextPosition());
	        }
	        else if (event instanceof UpdateRowsEventV2) {
	        	onUpdateRows((UpdateRowsEventV2)event);
	        	getCheckPoint().setSlaveLogPosition(event.getHeader().getNextPosition());
	        }
	        else if (event instanceof DeleteRowsEvent) {
	        	onDeleteRows((DeleteRowsEvent)event);
	        	getCheckPoint().setSlaveLogPosition(event.getHeader().getNextPosition());
	        }
	        else if (event instanceof DeleteRowsEventV2) {
	        	onDeleteRows((DeleteRowsEventV2)event);
	        	getCheckPoint().setSlaveLogPosition(event.getHeader().getNextPosition());
	        }
	        else if (event instanceof TableMapEvent) {
	        	onTableMap((TableMapEvent)event);
	        }
        }
        catch (Exception x) {
        	_log.error("replication failed: {}", event, x);
        	stop();
        }
	}

	private void onDeleteRows(DeleteRowsEvent event)  throws SQLException{
		_deleteCount++;
		long tableId = event.getTableId();
		TableMeta table = getTable(tableId);
		Integer pstmtId = deletePstmtMap.get(tableId);
		if (pstmtId == null) {
			pstmtId = buildDeletePstmt(table); 
			deletePstmtMap.put(tableId, pstmtId);
		}
		
		executeDeletePstmt(pstmtId, table, event.getRows());
	}

	private void onDeleteRows(DeleteRowsEventV2 event)  throws SQLException{
		_deleteCount++;
		long tableId = event.getTableId();
		TableMeta table = getTable(tableId);
		Integer pstmtId = deletePstmtMap.get(tableId);
		if (pstmtId == null) {
			pstmtId = buildDeletePstmt(table); 
			deletePstmtMap.put(tableId, pstmtId);
		}
		
		executeDeletePstmt(pstmtId, table, event.getRows());
	}

	private void onUpdateRows(UpdateRowsEvent event)  throws SQLException{
		_updateCount++;
		long tableId = event.getTableId();
		executeUpdatePstmt(tableId, event.getRows());
	}

	private void onUpdateRows(UpdateRowsEventV2 event)  throws SQLException{
		_updateCount++;
		long tableId = event.getTableId();
		executeUpdatePstmt(tableId, event.getRows());
	}

	private void onWriteRows(WriteRowsEvent event)  throws SQLException{
		_insertCount++;
		long tableId = event.getTableId();
		executeInsertPstmt(tableId, event.getRows());
	}

	private void onWriteRows(WriteRowsEventV2 event)  throws SQLException{
		_insertCount++;
		long tableId = event.getTableId();
		executeInsertPstmt(tableId, event.getRows());
	}

	private void onCommit(XidEvent event)  throws SQLException{
		_trxCounter++;
	}

	private void onQuery(QueryEvent event)  throws SQLException{
		String sql = event.getSql().toString();
		if (sql.equalsIgnoreCase("BEGIN")) {
			return;
		}
		String dbname = event.getDatabaseName().toString();
		if (!StringUtils.isEmpty(dbname)) {
			this.session.run("USE " + dbname);
		}
		this.session.run(sql);
		return;
	}

	private void onRotate(RotateEvent event) {
    	RotateEvent rotate = (RotateEvent)event;
    	getCheckPoint().setSlaveLogFile(rotate.getBinlogFilename());
    	getCheckPoint().setSlaveLogPosition(event.getBinlogPosition());
	}

	private void onTableMap(TableMapEvent event) {
		String[] name = new String[2];
		name[0] = event.getDatabaseName().toString();
		name[1] = event.getTableName().toString();
		long tableId = event.getTableId();
		this.tableById.put(tableId, name);
		
	};

	private TableMeta getTable(long tableId) {
		String[] name = this.tableById.get(tableId);
		if (name == null) {
			throw new RuntimeException("Table map not found for table id:" + tableId);
		}
		TableMeta table = getMetadata().getTable(Transaction.getSeeEverythingTrx(), name[0], name[1]);
		if (table == null) {
			throw new RuntimeException(name + " is not found in slave");
		}
		
		return table;
	}

	private MetadataService getMetadata() {
		return this.session.getOrca().getMetaService();
	}
	
	// assume binlog always carry all column value, and master and slave have matching columns 
    private int buildInsertPstmt(TableMeta meta) throws SQLException{
    	// build query
    	StringBuilder sql = new StringBuilder();
    	sql.append("INSERT INTO ");
    	sql.append(meta.getNamespace());
    	sql.append(".");
    	sql.append(meta.getTableName());
    	sql.append(" VALUES (?");

    	for (int i=0; i<meta.getColumns().size()-1; i++) {
    		sql.append(",?");
    	}
    	sql.append(")");
    	
    	String sqlStr = sql.toString();
    	
    	if (_log.isTraceEnabled()) {
    		_log.trace("PrepareStatement for insert:" + sqlStr);
    	}
    	PreparedStatement script = session.prepare(sqlStr);

        return script.hashCode();
    }

    private int buildDeletePstmt(TableMeta meta) throws SQLException{
    	// build query
    	StringBuilder sql = new StringBuilder();
    	sql.append("DELETE FROM ");
    	sql.append(meta.getNamespace());
    	sql.append(".");
    	sql.append(meta.getTableName());
    	sql.append(" WHERE ");

    	List<ColumnMeta> colMetas = null;
    	
    	// use only pk field for where clause if available
    	PrimaryKeyMeta pkMeta = meta.getPrimaryKey();
    	if (pkMeta==null){
    		colMetas = meta.getColumns();
    	} else {
    		colMetas = pkMeta.getColumns(meta);
    	}
    	for (ColumnMeta col: colMetas) {
    		sql.append('`');
    		sql.append(col.getColumnName());
    		sql.append('`');
    		sql.append("==");
    		sql.append("? and ");
    	}
    	
    	String sqlStr = sql.substring(0, sql.length()-4);
    	
    	if (_log.isTraceEnabled()) {
    		_log.trace("PrepareStatement for delete:" + sqlStr);
    	}
    	PreparedStatement script = session.prepare(sqlStr);

        return script.hashCode();
    }

    private int buildUpdatePstmt(TableMeta meta) throws SQLException{
    	// build query
    	StringBuilder sql = new StringBuilder();
    	sql.append("UPDATE ");
    	sql.append(meta.getNamespace());
    	sql.append(".");
    	sql.append(meta.getTableName());
    	sql.append(" SET ");

    	List<ColumnMeta> columns = meta.getColumns();
    	for (int i=0; i<columns.size(); i++) {
    		if (i>0) {
        		sql.append(",");
    		}
    		sql.append('`');
    		sql.append(meta.getColumns().get(i).getColumnName());
    		sql.append('`');
    		sql.append("=?");
    	}
    	
		sql.append(" WHERE ");

    	List<ColumnMeta> colMetas = null;
    	// use only pk field for where clause if available
    	PrimaryKeyMeta pkMeta = meta.getPrimaryKey();
    	if (pkMeta==null){
    		colMetas = meta.getColumns();
    	} else {
    		colMetas = pkMeta.getColumns(meta);
    	}
    	for (ColumnMeta col: colMetas) {
    		sql.append('`');
    		sql.append(col.getColumnName());
    		sql.append('`');
    		sql.append("==");
    		sql.append("? and ");
    	}
    	
    	String sqlStr = sql.substring(0, sql.length()-4);
    	
    	if (_log.isTraceEnabled()) {
    		_log.trace("PrepareStatement for update:" + sqlStr);
    	}
    	PreparedStatement script = session.prepare(sqlStr);

        return script.hashCode();
    }

	private void executeInsertPstmt(long tableId, List<Row> rows) throws SQLException {
		TableMeta table = getTable(tableId);
		Integer pstmtId = insertPstmtMap.get(tableId);
		if (pstmtId == null) {
			pstmtId = buildInsertPstmt(table); 
			insertPstmtMap.put(tableId, pstmtId);
		}
		executeInsertPstmt(pstmtId, rows);
	}

    private void executeInsertPstmt(int stmtId, List<Row> rows) {
    	PreparedStatement script = session.getPrepared(stmtId);
        for (Row row: rows) {
    		Parameters param = toParameters(row);
    		checkParameters(script, param);
    		try {
    			Object result = script.run(this.session, param);
    			checkResult(result);
    		}
    		catch (OrcaException x) {
    			if (x.getMessage().equals("EXISTENCE_VIOLATION")) {
    				_log.warn("INSERT failure is ignored");
    				return;
    			}
    			throw x;
    		}
        }
    }
    
    // for table with pk
    private void executeDeletePstmt(int stmtId, TableMeta meta, List<Row> rows) {
    	PreparedStatement script = session.getPrepared(stmtId);
        for (Row row: rows) {
    		Parameters param = toParameters(meta, row);
    		checkParameters(script, param);
            Object result = script.run(this.session, param);
    		if ((result instanceof Integer) && (((Integer)result) == 0)) {
    			_log.warn("delete failed with 0 update count {}", meta.getObjectName().toString());
    			_log.warn("{}", row);
    			return;
    		}
			checkResult(result);
        }
    }
    
	private void executeUpdatePstmt(long tableId, List<Pair<Row>> rows) throws SQLException {
		TableMeta table = getTable(tableId);
		Integer pstmtId = updatePstmtMap.get(tableId);
		if (pstmtId == null) {
			pstmtId = buildUpdatePstmt(table); 
			updatePstmtMap.put(tableId, pstmtId);
		}
		executeUpdatePstmt(tableId, pstmtId, table, rows);
	}

    private void executeUpdatePstmt(long tableId, int stmtId, TableMeta meta, List<Pair<Row>> rows) 
	throws SQLException {
    	PreparedStatement script = session.getPrepared(stmtId);
        for (Pair<Row> pair: rows) {
    		Parameters param = toParameters(meta, pair);
    		checkParameters(script, param);
    		Object result = script.run(this.session, param);
    		if ((result instanceof Integer) && (((Integer)result) == 0)) {
    			// fall back to insert if update failed
    			_log.warn("update failed with 0 update count {}, switching to insert", meta.getObjectName().toString());
    			_log.warn("{}", pair);
    			executeInsertPstmt(tableId, Collections.singletonList(pair.getAfter()));
    			return;
    		}
			checkResult(result);
        }
    }

    private void checkParameters(PreparedStatement script, Parameters params) {
        if (params.size() != script.getParameterCount()) {
        	throw new OrcaException(
        			"missmatching number of parameters {} {}", 
        			script.getParameterCount(), 
        			params.size());
        }
    }
    
    private void checkResult(Object result) {
    	if (result instanceof Integer) {
    		Integer n = (Integer)result;
    		if (n == 1) {
    			return;
    		}
    	}
    	throw new OrcaException("incorrect CRUD result: {}", result);
	}

	// map columns to parameter for insert pstmt
    private Parameters toParameters(Row row) {
    	List<Column> columns = row.getColumns();
    	Object[] pureValues = new Object[columns.size()];
        for (int i=0; i<columns.size(); i++) {
        	pureValues[i] = toParameter(columns.get(i));
        }
        return new Parameters(pureValues);
    }

    // map columns to parameter for delete pstmt with PK
    private Parameters toParameters(TableMeta meta, Row row) {
    	List<Column> columns = row.getColumns();
    	Object[] pureValues;
    	
    	PrimaryKeyMeta keyMeta = meta.getPrimaryKey();
    	if (keyMeta!=null) {
	    	List<ColumnMeta> primaryKeys = keyMeta.getColumns(meta);
	    	HashSet<Integer> pkNum = new HashSet<>();
	    	for (ColumnMeta key: primaryKeys)
	    	{
	    		pkNum.add(key.getColumnId());
	    	}
	    	
	        pureValues = new Object[pkNum.size()];
	        for (int i=0; i<columns.size(); i++) {
	        	// col id starts with 1
	        	if (pkNum.contains(i+1))
	        	{
	        		pureValues[i] = toParameter(columns.get(i));
	        	}
	        }
    	} else {
    		pureValues = new Object[columns.size()];
            for (int i=0; i<columns.size(); i++) {
            	pureValues[i] = toParameter(columns.get(i));
            }    		
    	}
        return new Parameters(pureValues);
    }

    // map columns to parameter for update pstmt with PK
    private Parameters toParameters(TableMeta meta, Pair<Row> pair) {
    	List<Column> colsAft = pair.getAfter().getColumns();
    	List<Column> colsBef = pair.getBefore().getColumns();
    	
    	Object[] pureValues;
    	
    	PrimaryKeyMeta keyMeta = meta.getPrimaryKey();
    	if (keyMeta!=null) {
	    	List<ColumnMeta> primaryKeys = keyMeta.getColumns(meta);
	    	// generate set for pk column id 
	    	HashSet<Integer> pkNum = new HashSet<>();
	    	for (ColumnMeta key: primaryKeys)
	    	{
	    		pkNum.add(key.getColumnId());
	    	}
	    	
	        pureValues = new Object[colsAft.size() + pkNum.size()];
	        
	        for (int i=0; i<colsAft.size(); i++) {
	        	pureValues[i] = toParameter(colsAft.get(i));
	        }
        	// appending parameters for where clause
	        for (int i=0; i<colsBef.size(); i++) {
	        	// col id starts with 1
	        	if (pkNum.contains(i+1))
	        	{
	        		pureValues[i+colsAft.size()] = toParameter(colsBef.get(i));
	        	}
	        }
    	} else {
	        pureValues = new Object[colsAft.size() + colsBef.size()];
	        
	        for (int i=0; i<colsAft.size(); i++) {
	        	pureValues[i] = toParameter(colsAft.get(i));
	        }
        	// appending parameters for where clause
	        for (int i=0; i<colsBef.size(); i++) {
        		pureValues[i+colsAft.size()] = toParameter(colsBef.get(i));
	        }
    		
    	}
        return new Parameters(pureValues);
    }

	private Object toParameter(Column col) {
		Object value;
		if (col instanceof StringColumn){
			// use UTF-8 to match open replicator default charset
			value = new String((byte[])col.getValue(), Charsets.UTF_8);
		} else {
			value = col.getValue();
		}
			
		if (!(value instanceof java.util.Date)) {
			return value;
		}
		if (value instanceof java.sql.Date || value instanceof java.sql.Timestamp) {
			return value;
		}
		// open replicator sometimes use java.util.Date instead of java.sql.Date. 
		value = new Timestamp(((java.util.Date)value).getTime());
		return value;
	}

}
