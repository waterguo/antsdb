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

import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.VaporizingRow;
import com.antsdb.saltedfish.server.mysql.ErrorMessage;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.Session;
import com.antsdb.saltedfish.sql.vdm.KeyMaker;
import com.antsdb.saltedfish.sql.vdm.Transaction;
import com.antsdb.saltedfish.sql.vdm.VdmContext;

/**
 * 
 * @author *-xguo0<@
 */
public class ForeignKeyConstraint extends Constraint {
    private ForeignKeyMeta meta;
    private GTable parent;
    private TableMeta table;
    private KeyMaker keymaker;

    public ForeignKeyConstraint(TableMeta table, ForeignKeyMeta meta) {
        this.meta = meta;
        this.table = table;
    }
    
    public void prepare(GeneratorContext ctx) {
        prepare(ctx.getSession());
    }
    
    public void prepare(Session session) {
        MetadataService metaservice = session.getOrca().getMetaService();
        RuleMeta<?> parentKey = findParentKey(session, metaservice, this.meta);
        if (parentKey instanceof PrimaryKeyMeta) {
            TableMeta parentMeta = metaservice.getTable(session.getTransaction(), parentKey.getTableId());
            this.parent = session.getOrca().getHumpback().getTable(parentMeta.getHtableId());
        }
        else if (parentKey instanceof IndexMeta) {
            this.parent = session.getOrca().getHumpback().getTable(((IndexMeta)parentKey).getIndexTableId());
        }
        else {
            throw new IllegalArgumentException();
        }
        this.keymaker = new KeyMaker(this.meta.getColumns(this.table), true);
    }
    
    @Override
    public void check(VdmContext ctx, Heap heap, VaporizingRow row) {
        if (this.keymaker.isNull(row)) {
            return;
        }
        Transaction trx = ctx.getTransaction();
        long pKey = this.keymaker.make(heap, row);
        long pRow = this.parent.getMemTable().get(trx.getTrxId(), trx.getTrxTs(), pKey, 0);
        if (pRow == 0) {
            throw new ErrorMessage(
                    1452, 
                    "Cannot add or update a child row: a foreign key constraint fails %s:%s",
                    this.parent.getName(),
                    KeyBytes.toString(pKey));
        }
    }
    
    private static RuleMeta<?> findParentKey(Session session, MetadataService meta, ForeignKeyMeta fk) {
        // find table
        TableMeta table = meta.getTable(session.getTransaction(), fk.getParentTable());
        if (table == null) {
            throw new OrcaException("parent table {} is not found", fk.getParentTable());
        }
        
        // find key
        RuleMeta<?> result = null;
        PrimaryKeyMeta pk = table.getPrimaryKey();
        if (pk != null) {
            if (pk.conform(table, fk.getParentColumns())) {
                result = pk;
            }
        }
        if (result == null) {
            for (IndexMeta i:table.getIndexes()) {
                if (!i.isUnique()) {
                    continue;
                }
                if (i.conform(table, fk.getParentColumns())) {
                    result = i;
                    break;
                }
            }
        }
        return result;
    }
    
}
