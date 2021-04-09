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
package com.antsdb.saltedfish.sql.vdm;

import java.util.ArrayList;
import java.util.List;

import com.antsdb.saltedfish.cpp.BluntHeap;
import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.RowIterator;
import com.antsdb.saltedfish.nosql.TableType;
import com.antsdb.saltedfish.sql.LockLevel;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.IndexMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.util.Pair;

public class CreateIndex extends Statement {
    String indexName;
    ObjectName tableName;
    public List<Pair<String, Integer>> columns;
    boolean isUnique;
    boolean createIfNotExists;
    private boolean isFullText;
    private boolean rebuild = true;
    
    public CreateIndex(String indexName,
                       boolean isFullText,
                       boolean isUnique, 
                       boolean createIfNotExists, 
                       ObjectName tableName, 
                       List<Pair<String, Integer>> columns) {
        super();
        this.indexName = indexName;
        this.tableName = tableName;
        this.columns = columns;
        this.isUnique = isUnique;
        this.createIfNotExists = createIfNotExists;
        this.isFullText = isFullText;
    }
    
    @Override
    public Object run(VdmContext ctx, Parameters params) {
        // lock the table exclusively
        
        TableMeta table = Checks.tableExist(ctx.getSession(), this.tableName);
        try {
            ctx.session.lockTable(table.getId(), LockLevel.EXCLUSIVE, false);
            
            // make the meta data change
            
            create(ctx, params);
            return null;
        }
        finally {
            ctx.session.unlockTable(table.getId());
        }
    }

    private void create(VdmContext ctx, Parameters params) {
        TableMeta table = Checks.tableExist(ctx.getSession(), this.tableName);
        
        // get IndexMeta if already exists
        IndexMeta existingIndexMeta = null;
        for (IndexMeta i:table.getIndexes()) {
            if (indexName.equals(i.getName())) {
                existingIndexMeta = i;
                break;
            }
        }
        
        // exit or throw exception for existing
        if (existingIndexMeta != null) {
            if (this.createIfNotExists) {
                return;
            }
            else {
                throw new OrcaException("Index already exists: " + indexName);
            }
        }

        // create new index
        
        List<ColumnMeta> columns = new ArrayList<>();
        List<Integer> prefix = new ArrayList<>();
        for (Pair<String, Integer> i:this.columns) {
            ColumnMeta col = table.getColumn(i.x);
            if (col == null) {
                throw new OrcaException("column that makes of the index is not found: " + i);
            }
            columns.add(col);
            prefix.add(i.y);
        }
        createIndex(ctx, table, indexName, isFullText, isUnique, columns, prefix, this.rebuild);
    }

    static IndexMeta createIndex(
            VdmContext ctx, 
            TableMeta table, 
            String indexName, 
            boolean isFullText,
            boolean isUnique, 
            List<ColumnMeta> columns,
            List<Integer> prefix,
            boolean rebuild) {
        // create new index
        IndexMeta index = new IndexMeta(ctx.getOrca(), table);
        index.setName(indexName);
        index.setNamespace(table.getNamespace());
        index.setUnique(isUnique);
        int id = (int)ctx.getOrca().getIdentityService().getNextGlobalId();
        index.setIndexTableId(id);
        index.genUniqueExternalName(table, indexName, id);
        index.setFullText(isFullText);
        index.setRuleColumns(columns);
        index.setPrefix(prefix);
        if (table.isTemproray()) {
            index.setIndexTableId(-index.getIndexTableId());
        }
        ctx.getMetaService().addRule(ctx.getHSession(), ctx.getTransaction(), index);
        
        // create physical table
        Humpback humpback = ctx.getOrca().getHumpback();
        humpback.createTable(
                ctx.getHSession(),
                table.getNamespace(),
                index.getExternalName(),
                index.getIndexTableId(),
                TableType.INDEX);
        
        // if table is not empty, lots of shit to do
        if (rebuild) {
            buildIndex(ctx, table, index);
        }
        
        //
        return index;
    }
    
    static void buildIndex(VdmContext ctx, TableMeta table, IndexMeta index) {
        Transaction trx = ctx.getTransaction();
        GTable gtable = ctx.getHumpback().getTable(table.getHtableId());
        GTable gindex = ctx.getHumpback().getTable(index.getIndexTableId());
        KeyMaker keyMaker = new KeyMaker(index.getColumns(table), index.isUnique());
        try (BluntHeap heap = new BluntHeap()) {
            RowIterator scanner = gtable.scan(trx.getTrxId(), trx.getTrxTs(), true);
            while (scanner.next()) {
                heap.reset(0);
                Row row = scanner.getRow();
                long pIndexKey = keyMaker.make(heap, row);
                gindex.insertIndex(ctx.getHSession(), trx.getTrxId(), pIndexKey, row.getKeyAddress(), (byte)0, 0);
            }
        }
    }

    public void setRebuild(boolean value) {
        this.rebuild  = value;
    }

}
