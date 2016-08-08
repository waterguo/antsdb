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
package com.antsdb.saltedfish.sql.vdm;

import java.util.ArrayList;
import java.util.List;

import com.antsdb.saltedfish.cpp.BluntHeap;
import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.RowIterator;
import com.antsdb.saltedfish.sql.LockLevel;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.IndexMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;

public class CreateIndex extends Statement {
    String indexName;
    ObjectName tableName;
    List<String> columns;
    boolean isUnique;
    boolean createIfNotExists;
    
    public CreateIndex(String indexName, boolean isUnique, boolean createIfNotExists, 
    					ObjectName tableName, List<String> columns) {
        super();
        this.indexName = indexName;
        this.tableName = tableName;
        this.columns = columns;
        this.isUnique = isUnique;
        this.createIfNotExists = createIfNotExists;
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
        for (String i:this.columns) {
            ColumnMeta col = table.getColumn(i);
            if (col == null) {
                throw new OrcaException("column that makes of the index is not found: " + i);
            }
            columns.add(col);
        }
        createIndex(ctx, table, indexName, isUnique, columns);
    }

	static IndexMeta createIndex(
			VdmContext ctx, 
			TableMeta table, 
			String indexName, 
			boolean isUnique, 
			List<ColumnMeta> columns) {
        // create new index
        IndexMeta index = new IndexMeta(ctx.getOrca(), table);
        index.setName(indexName);
        index.setUnique(isUnique);
        index.setIndexTableId((int)ctx.getOrca().getIdentityService().getSequentialId(TableMeta.SEQ_NAME));
        index.setExternalName(table, indexName);
        for (ColumnMeta col:columns) {
            index.addColumn(ctx.getOrca(), col);
        }
        ctx.getMetaService().addRule(ctx.getTransaction(), index);
        
        // create physical table

        Humpback humpback = ctx.getOrca().getStroageEngine();
        humpback.createTable(
        		table.getNamespace(),
        		index.getExternalName(),
        		index.getIndexTableId());
        
        // if table is not empty, lots of shit to do
        
        buildIndex(ctx, table, index);
        
        //
        
        return index;
	}
	
	static void buildIndex(VdmContext ctx, TableMeta table, IndexMeta index) {
		Transaction trx = ctx.getTransaction();
        GTable gtable = ctx.getHumpback().getTable(table.getId());
        GTable gindex = ctx.getHumpback().getTable(index.getIndexTableId());
        KeyMaker keyMaker = new KeyMaker(index.getColumns(table), index.isUnique());
		try (BluntHeap heap = new BluntHeap()) {
			RowIterator scanner = gtable.scan(trx.getTrxId(), trx.getTrxTs());
			while (scanner.next()) {
				Row row = scanner.getRow();
				long pIndexKey = keyMaker.make(heap, row);
				gindex.insertIndex(trx.getTrxId(), pIndexKey, row.getKeyAddress(), 0);
			}
		}
	}

}
