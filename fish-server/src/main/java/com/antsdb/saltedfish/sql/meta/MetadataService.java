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
package com.antsdb.saltedfish.sql.meta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.Value;
import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.HumpbackError;
import com.antsdb.saltedfish.nosql.HumpbackException;
import com.antsdb.saltedfish.nosql.HumpbackSession;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.RowIterator;
import com.antsdb.saltedfish.nosql.ScanOptions;
import com.antsdb.saltedfish.nosql.SlowRow;
import com.antsdb.saltedfish.nosql.TrxMan;
import com.antsdb.saltedfish.sql.NativeAuthPlugin;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.meta.RuleMeta.Rule;
import com.antsdb.saltedfish.sql.vdm.KeyMaker;
import com.antsdb.saltedfish.sql.vdm.ObjectName;
import com.antsdb.saltedfish.sql.vdm.Transaction;
import com.antsdb.saltedfish.util.UberUtil;

import static com.antsdb.saltedfish.sql.OrcaConstant.*;

/**
 * meta data service
 *  
 * @author *-xguo0<@
 */
public class MetadataService {
    public static final String SYSTABLES = TABLENAME_SYSTABLE;
    public static final String SYSCOLUMNS = TABLENAME_SYSCOLUMN;
    public static final String SYSSEQUENCES = TABLENAME_SYSSEQUENCE;

    static Logger _log = UberUtil.getThisLogger();
    
    Orca orca;
    Map<Integer, TableMeta> cache = new ConcurrentHashMap<>();
    Map<ObjectName, SequenceMeta> seqCache = new ConcurrentHashMap<>();
    private long version;
    GTable gtableSeq;
    private TrxMan trxman;
    
    public MetadataService(Orca orca) {
        super();
        this.orca = orca;
        this.trxman = orca.getTrxMan();
    }
    
    public TableMeta getTable(Transaction trx, int id) {
        // are we in a transaction? read it from database if it is
        long trxid = trx.getTrxId();
        if (trxid != 0) {
            if (trx.isDddl()) {
                return loadTable(trx, id);
            }
        }
        
        // is it in the cache?
        TableMeta tableMeta = this.cache.get(id);
        if (tableMeta != null) {
            return tableMeta;
        }
        
        // if not load it from storage
        synchronized(this) {
            tableMeta = loadTable(trx, id);
            if (tableMeta != null) {
                if (tableMeta.getHtableId() >= 0) {
                    this.cache.put(tableMeta.getId(), tableMeta);
                }
                else if (tableMeta.isTemproray()) {
                    this.cache.put(tableMeta.getId(), tableMeta);
                }
                else {
                    id = -tableMeta.getHtableId();
                    TableMeta realTableMeta = getTable(trx, id);
                    if (realTableMeta != null) {
                        this.cache.put(tableMeta.getId(), realTableMeta);
                    }
                    tableMeta = realTableMeta;
                }
            }
            return tableMeta;
        }
    }
    
    public TableMeta getTable(Transaction trx, ObjectName tableName) {
        return getTable(trx, tableName.getNamespace(), tableName.getTableName());
    }
    
    public TableMeta getTable(Transaction trx, String ns, String tableName) {
        // are we in a transaction? read it from database if it is
        
        long trxid = trx.getTrxId();
        if (trxid != 0) {
            if (trx.isDddl()) {
                return loadTable(trx, ns, tableName);
            }
        }
        
        // is it in the cache?
        
        for (TableMeta i:this.cache.values()) {
            if (!i.getNamespace().equalsIgnoreCase(ns)) {
                continue;
            }
            if (!i.getTableName().equalsIgnoreCase(tableName)) {
                continue;
            }
            return i;
        }
        
        // if not load it from storage
        
        synchronized(this) {
            TableMeta tableMeta = loadTable(trx, ns, tableName);
            if (tableMeta != null) {
                this.cache.put(tableMeta.getId(), tableMeta);
            }
            return tableMeta;
        }
    }

    private TableMeta loadTable(Transaction trx, int id) {
        if (id <= TableId.MAX) {
            return null;
        }
        GTable table = this.orca.getHumpback().getTable(Orca.SYSNS, TABLEID_SYSTABLE);
        Row rrow = table.getRow(trx.getTrxId(), trx.getTrxTs(), KeyMaker.make(id));
        if (rrow == null) {
            return null;
        }
        SlowRow row = SlowRow.from(rrow);
        row.setMutable(false);
        return loadTable(trx, row);
    }
    
    private TableMeta loadTable(Transaction trx, String ns, String tableName) {
        // if we in transaction use trxid, otherwise use latest visible
        long trxid = trx.getTrxId();
        long trxts = Long.MAX_VALUE;
        GTable table = this.orca.getHumpback().getTable(Orca.SYSNS, TABLEID_SYSTABLE);
        for (RowIterator i=table.scan(trxid, trxts, true); i.next();) {
            Row rrow = i.getRow();
            if (rrow == null) {
                return null;
            }
            SlowRow row = SlowRow.from(rrow);
            row.setMutable(false);
            if (!ns.equalsIgnoreCase((String)row.get(ColumnId.systable_namespace.getId()))) {
                continue;
            }
            if (!tableName.equalsIgnoreCase((String)row.get(ColumnId.systable_table_name.getId()))) {
                continue;
            }
            TableMeta result = loadTable(trx, row);
            _log.trace("loaded table {}/{}.{} metadata from {}", result.getId(), ns, tableName, i.toString());
            return result;
        }
        return null;
    }
    
    private TableMeta loadTable(Transaction trx, SlowRow row) {
        TableMeta tableMeta = new TableMeta(row);
        loadColumns(trx, tableMeta);
        loadRules(trx, tableMeta);
     
        // create primary key maker
        
        if (tableMeta.pk != null) {
            tableMeta.keyMaker = new KeyMaker(tableMeta.pk.getColumns(tableMeta), true);
        }
        else {
            tableMeta.keyMaker = new KeyMaker(Collections.emptyList(), true);
        }
        
        // create index key maker
        
        for (IndexMeta i:tableMeta.indexes) {
            KeyMaker maker = new KeyMaker(i.getColumns(tableMeta), i.isUnique());
            i.keyMaker = maker;
        }
        
        // done
        
        return tableMeta;
    }
    
    private void loadRules(Transaction trx, TableMeta tableMeta) {
        int tableId = tableMeta.getId();
        GTable table = this.orca.getHumpback().getTable(Orca.SYSNS, TABLEID_SYSRULE);
        if (table == null) {
            return;
        }
        byte[] start = KeyMaker.gen(tableMeta.getId());
        byte[] end = KeyMaker.gen(tableMeta.getId() + 1);
        long options = ScanOptions.excludeEnd(0);
        for (RowIterator i=table.scan(trx.getTrxId(), Long.MAX_VALUE, start, end, options); i.next();) {
            Row rrow = i.getRow();
            SlowRow row = SlowRow.from(rrow);
            if (row == null) {
                break;
            }
            row.setMutable(false);
            if (tableId != (int)row.get(ColumnId.sysrule_table_id.getId())) {
                continue;
            }
            int type = (Integer)row.get(ColumnId.sysrule_rule_type.getId());
            if (type == Rule.PrimaryKey.ordinal()) {
                PrimaryKeyMeta pk = new PrimaryKeyMeta(row);
                tableMeta.pk = pk;
            }
            else if (type == Rule.Index.ordinal()) {
                IndexMeta index = new IndexMeta(row);
                tableMeta.getIndexes().add(index);
            }
            else if (type == Rule.ForeignKey.ordinal()) {
                ForeignKeyMeta fk = new ForeignKeyMeta(row);
                tableMeta.getForeignKeys().add(fk);
            }
            else {
                throw new NotImplementedException();
            }
        }
    }

    private void loadColumns(Transaction trx, TableMeta tableMeta) {
        GTable table = this.orca.getHumpback().getTable(Orca.SYSNS, TABLEID_SYSCOLUMN);
        List<ColumnMeta> list = new ArrayList<>();
        byte[] from = KeyMaker.gen(tableMeta.getId());
        byte[] to = KeyMaker.gen(tableMeta.getId() + 1);
        long options = ScanOptions.excludeEnd(0);
        for (RowIterator ii = table.scan(trx.getTrxId(), Long.MAX_VALUE, from, to, options); ii.next();) {
            SlowRow i = SlowRow.from(ii.getRow());
            if (i == null) {
                break;
            }
            i.setMutable(false);
            ColumnMeta columnMeta = new ColumnMeta(this.orca.getTypeFactory(), i);
            if (!columnMeta.getNamespace().equals(tableMeta.getNamespace())) {
                continue;
            }
            if (!columnMeta.getTableName().equals(tableMeta.getTableName())) {
                continue;
            }
            if (columnMeta.getTableId() != tableMeta.getId()) {
                _log.warn("column {} @ {} is corrupted", columnMeta.getId(), ii.toString());
                continue;
            }
            list.add(columnMeta);
        }
        list.sort((col1, col2)-> {
            float delta = col1.getSequence() - col2.getSequence();
            if (delta > 0) {
                return 1;
            }
            else if (delta < 0) {
                return -1;
            }
            else {
                return 0;
            }
        });
        tableMeta.setColumns(list);
    }
    
    public void addTable(HumpbackSession hsession, Transaction trx, TableMeta tableMeta) throws HumpbackException {
        long trxid = trx.getGuaranteedTrxId();
        GTable sysTable = getSysTable();
        tableMeta.row.setTrxTimestamp(trxid);
        sysTable.insert(hsession, tableMeta.row, 0);

        // doh, this is a ddl transaction, remember it
        
        trx.setDdl(true);
    }

    public GTable getSysTable() {
        Humpback humpback = this.orca.getHumpback();
        return humpback.getTable(Orca.SYSNS, TABLEID_SYSTABLE);
    }

    public GTable getSysColumn() {
        Humpback humpback = this.orca.getHumpback();
        return humpback.getTable(Orca.SYSNS, TABLEID_SYSCOLUMN);
    }

    GTable getSysRule() {
        Humpback humpback = this.orca.getHumpback();
        return humpback.getTable(Orca.SYSNS, TABLEID_SYSRULE);
    }

    GTable getSysUser() {
        Humpback humpback = this.orca.getHumpback();
        return humpback.getTable(Orca.SYSNS, TABLEID_SYSUSER);
    }
    
    public void dropTable(HumpbackSession hsession, Transaction trx, TableMeta tableMeta) throws HumpbackException {
        long trxid = trx.getGuaranteedTrxId();
        GTable table = getSysTable();
        HumpbackError error = table.delete(hsession, trxid, tableMeta.getKey(), 1000);
        String location = table.getLocation(trxid, Long.MAX_VALUE, tableMeta.getKey());
        _log.debug("table {}/{} is deleted at {}", trx, tableMeta.getObjectName().toString(), location);
        if (error != HumpbackError.SUCCESS) {
            throw new OrcaException(error);
        }
        
        GTable sysColumn = getSysColumn();
        for (ColumnMeta column:tableMeta.columns) {
            sysColumn.delete(hsession, trxid, column.getKey(), 1000);
        }
        
        if (tableMeta.getPrimaryKey() != null) {
            deletePrimaryKey(hsession, trx, tableMeta.getPrimaryKey());
        }

        // doh, this is a ddl transaction, remember it
        
        trx.setDdl(true);
    }
    
    private void deletePrimaryKey(HumpbackSession hsession, Transaction trx, PrimaryKeyMeta pk) {
        long trxid = trx.getGuaranteedTrxId();
        GTable ruleTable = this.orca.getHumpback().getTable(Orca.SYSNS, TABLEID_SYSRULE);
        ruleTable.delete(hsession, trxid, pk.row.getKey(), 1000);
        
        // doh, this is a ddl transaction, remember it
        
        trx.setDdl(true);
    }

    public void addColumn(HumpbackSession hsession, Transaction trx, TableMeta table, ColumnMeta columnMeta) 
    throws HumpbackException {
        // add column metadata
        
        long trxid = trx.getGuaranteedTrxId();
        GTable sysColumn = getSysColumn();
        columnMeta.row.setTrxTimestamp(trxid);
        sysColumn.insert(hsession, columnMeta.row, 0);

        // doh, this is a ddl transaction, remember it
        trx.setDdl(true);
        
        // inform humpback
        Humpback humpback = this.orca.getHumpback();
        int columnPos = columnMeta.getColumnId();
        String columnName = columnMeta.getColumnName();
        if (table.getHtableId() >= 0) {
            humpback.addColumn(hsession, trxid, table.getHtableId(), columnPos, columnName);
            int fishType = columnMeta.getDataType().getFishType();
            if ((fishType == Value.TYPE_BLOB) || (fishType == Value.TYPE_CLOB)) {
                humpback.addColumn(hsession, trxid, table.getBlobTableId(), columnPos, columnName);
            }
        }
    }
    
    public void modifyColumn(HumpbackSession hsession, Transaction trx, ColumnMeta columnMeta) 
    throws HumpbackException {
        long trxid = trx.getGuaranteedTrxId();
        GTable sysColumn = getSysColumn();
        sysColumn.update(hsession, trxid, columnMeta.row, 0);

        // doh, this is a ddl transaction, remember it
        
        trx.setDdl(true);
    }
    
    public void deleteColumn(HumpbackSession hsession, Transaction trx, TableMeta table, ColumnMeta column) {
        long trxid = trx.getGuaranteedTrxId();
        GTable sysColumn = getSysColumn();
        sysColumn.delete(hsession, trxid, column.row.getKey(), 1000);

        // doh, this is a ddl transaction, remember it
        
        trx.setDdl(true);
        
        // inform humpback
        
        Humpback humpback = this.orca.getHumpback();
        humpback.deleteColumn(hsession, trxid, table.getHtableId(), column.getColumnId());
        if (column.isBlobClob()) {
            if (humpback.getTable(table.getBlobTableId()) != null) {
                humpback.deleteColumn(hsession, trxid, table.getBlobTableId(), column.getColumnId());
            }
        }
    }
    
    public List<String> getNamespaces() {
        return this.orca.getHumpback().getNamespaces();
    }

    public List<String> getTables(Transaction trx, String ns) {
        List<String> tables = new ArrayList<String>();
        GTable table = this.orca.getHumpback().getTable(Orca.SYSNS, TABLEID_SYSTABLE);
        for (RowIterator i=table.scan(trx.getTrxId(), trx.getTrxTs(), true); i.next();) {
            SlowRow row = SlowRow.from(i.getRow());
            if (row == null) {
                break;
            }
            if (ns.equalsIgnoreCase((String)(row.get(ColumnId.systable_namespace.getId())))) {
                tables.add((String)row.get(ColumnId.systable_table_name.getId()));
            }
        }
        return tables;
    }
    
    SequenceMeta loadSequence(Transaction trx, ObjectName name) {
        Row raw = getSequenceTable().getRow(
                trx.getTrxId(), 
                Long.MAX_VALUE, 
                KeyMaker.make(name.getNamespace() + "." + name.getTableName()));
        SlowRow row = SlowRow.from(raw);
        if (row == null) {
            return null;
        }
        SequenceMeta seq = new SequenceMeta(row);
        return seq;
    }
    
    public SequenceMeta getSequence(Transaction trx, ObjectName name) {
        // load from database is this is a ddl trx
        
        if (trx.isDddl()) {
            return loadSequence(trx, name);
        }
        
        // find it from cache
        
        SequenceMeta seq = this.seqCache.get(name);
        if (seq == null) {
            synchronized (this) {
                seq = this.seqCache.get(name);
                if (seq == null) {
                    seq = loadSequence(trx, name);
                    if (seq != null) {
                        this.seqCache.put(name, seq);
                    }
                }
            }
        }
        return seq;
    }
    
    public void addSequence(HumpbackSession hsession, Transaction trx, SequenceMeta seq) {
        long trxid = trx.getGuaranteedTrxId();
        GTable table = this.orca.getHumpback().getTable(Orca.SYSNS, TABLEID_SYSSEQUENCE);
        seq.row.setTrxTimestamp(trxid);
        HumpbackError error = table.insert(hsession, seq.row, 0);
        if (error != HumpbackError.SUCCESS) {
            throw new OrcaException(error);
        }

        // doh, this is a ddl transaction, remember it
        
        trx.setDdl(true);
    }
    
    public void dropSequence(HumpbackSession hsession, Transaction trx, SequenceMeta seq) {
        long trxid = trx.getGuaranteedTrxId();
        getSequenceTable().delete(hsession, trxid, seq.getKey(), 1000);

        // doh, this is a ddl transaction, remember it
        
        trx.setDdl(true);
        this.seqCache.remove(new ObjectName(seq.getNamespace(), seq.getSequenceName()));
    }

    public void updateSequence(HumpbackSession hsession, long trxid, SequenceMeta seq) {
        GTable table = this.orca.getHumpback().getTable(Orca.SYSNS, TABLEID_SYSSEQUENCE);
        HumpbackError error = table.update(hsession, trxid, seq.row, 0);
        if (error != HumpbackError.SUCCESS) {
            throw new OrcaException(error);
        }
        this.seqCache.remove(seq.getObjectName());
    }

    public long nextSequence(HumpbackSession hsession, ObjectName name) {
        SequenceMeta seq = getSequence(Transaction.getSeeEverythingTrx(), name);
        if (seq == null) {
            throw new IllegalArgumentException();
        }
        return seq.next(hsession, getSequenceTable(), this.trxman);
    }
    
    public long nextSequence(HumpbackSession hsession, ObjectName name, int increment) {
        SequenceMeta seq = getSequence(Transaction.getSeeEverythingTrx(), name);
        if (seq == null) {
            throw new IllegalArgumentException();
        }
        return seq.next(hsession, getSequenceTable(), this.trxman, increment);
    }

    public void addRule(HumpbackSession hsession, Transaction trx, RuleMeta<?> rule) {
        GTable ruleTable = this.orca.getHumpback().getTable(Orca.SYSNS, TABLEID_SYSRULE);
        rule.row.setTrxTimestamp(trx.getGuaranteedTrxId());
        ruleTable.insert(hsession, rule.row, 0);

        // doh, this is a ddl transaction, remember it
        
        trx.setDdl(true);
    }
    
    public void updateRule(HumpbackSession hsession, Transaction trx, RuleMeta<?> rule) {
        GTable ruleTable = this.orca.getHumpback().getTable(Orca.SYSNS, TABLEID_SYSRULE);
        rule.row.setTrxTimestamp(trx.getGuaranteedTrxId());
        ruleTable.update(hsession, trx.getGuaranteedTrxId(), rule.row, 0);

        // doh, this is a ddl transaction, remember it
        
        trx.setDdl(true);
    }

    public synchronized void commit(Transaction trx, long version) {
        if (version > 0) {
            this.version = version;
        }
        Map<Integer, TableMeta> aged = this.cache;
        this.cache = new ConcurrentHashMap<>(); 
        for (TableMeta i:aged.values()) {
            i.isAged = true;
        }
        
        // reset statement cache cuz statement may refer to meta-data and now they are aged.
        
        this.orca.clearStatementCache();
    }

    public void deleteRule(HumpbackSession hsession, Transaction trx, RuleMeta<?> rule) {
        trx.setDdl(true);
        long trxid = trx.getGuaranteedTrxId();
        GTable sysRule = getSysRule();
        sysRule.delete(hsession, trxid, rule.row.getKey(), 0);

        // doh, this is a ddl transaction, remember it
        
        trx.setDdl(true);
    }

    /**
     * find a new unique name using the input as prefix
     * 
     * @param tableName
     * @return
     */
    public ObjectName findUniqueName(Transaction trx, ObjectName tableName) {
        for (;;) {
            if (getTable(trx, tableName) == null) {
                return tableName;
            }
            tableName.table = tableName.table + "_";
        }
    }

    public long getVersion() {
        return this.version;
    }

    public void updateTable(HumpbackSession hsession, Transaction trx, TableMeta table) {
        trx.getGuaranteedTrxId();
        GTable systable = this.orca.getHumpback().getTable(Orca.SYSNS, TABLEID_SYSTABLE);
        HumpbackError error = systable.update(hsession, trx.getGuaranteedTrxId(), table.row, 0);
        if (error != HumpbackError.SUCCESS) {
            throw new OrcaException(error);
        }
        trx.setDdl(true);
    }

    public void updateIndex(HumpbackSession hsession, Transaction trx, IndexMeta index) {
        trx.getGuaranteedTrxId();
        GTable sysrule = this.orca.getHumpback().getTable(Orca.SYSNS, TABLEID_SYSRULE);
        HumpbackError error = sysrule.update(hsession, trx.getGuaranteedTrxId(), index.row, 0);
        if (error != HumpbackError.SUCCESS) {
            throw new OrcaException(error);
        }
        trx.setDdl(true);
    }

    public UserMeta getUser(String user) {
        GTable sysuser = getSysUser();
        for (RowIterator i=sysuser.scan(0, Long.MAX_VALUE, true);;) {
            if (!i.next()) {
                break;
            }
            Row row = i.getRow();
            if (user.equals(row.get(ColumnId.sysuser_name.getId()))) {
                UserMeta result = new UserMeta(SlowRow.from(row));
                if (!result.isDeleted()) {
                    return result;
                }
            }
        }
        return null;
    }
    
    public void setPassword(HumpbackSession hsession, String user, String password) {
        GTable sysuser = getSysUser();
        UserMeta userMeta = getUser(user);
        if (userMeta == null) {
            userMeta = new UserMeta((int)this.orca.getIdentityService().getNextGlobalId());
        }
        byte[] hash = new NativeAuthPlugin(orca).hash(password);
        userMeta.setName(user);
        userMeta.setPassword(hash);
        sysuser.put(hsession, 1, userMeta.row, 0);
    }
    
    public void dropUser(HumpbackSession hsession, String user) {
        GTable sysuser = getSysUser();
        UserMeta userMeta = getUser(user);
        if (userMeta == null) {
            throw new OrcaException("user {} is not found", user);
        }
        userMeta.setDeleteMark(true);
        sysuser.update(hsession, 1, userMeta.row, 0);
    }
    
    public void close() {
        HumpbackSession hsession = this.orca.getHumpback().createSession("local/metadata");
        hsession.open();
        try {
            for (SequenceMeta i:this.seqCache.values()) {
                i.closeAndUpdate(hsession, getSequenceTable(), this.trxman);
            }
            this.seqCache = null;
        }
        finally {
            this.orca.getHumpback().deleteSession(hsession);
        }
    }
    
    private GTable getSequenceTable() {
        if (this.gtableSeq == null) {
            this.gtableSeq = this.orca.getHumpback().getTable(Orca.SYSNS, TABLEID_SYSSEQUENCE);
        }
        return this.gtableSeq;
    }

    public void clearCache() {
        this.cache.clear();
        this.orca.clearStatementCache();
    }
}
