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

import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;

import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.TableType;
import com.antsdb.saltedfish.sql.LockLevel;
import com.antsdb.saltedfish.sql.meta.IndexMeta;
import com.antsdb.saltedfish.sql.meta.MetadataService;
import com.antsdb.saltedfish.sql.meta.SequenceMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.util.UberUtil;

public class TruncateTable extends Statement {
    static Logger _log = UberUtil.getThisLogger();
    
    ObjectName tableName;

    public TruncateTable(ObjectName tableName) {
        super();
        this.tableName = tableName;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params) {
        Humpback humpback = ctx.getOrca().getHumpback();
        MetadataService meta = ctx.getMetaService();
        TableMeta table = Checks.tableExist(ctx.getSession(), this.tableName);
        try {
            // acquire exclusive lock
            Transaction trx = ctx.getTransaction(); 
            trx.getGuaranteedTrxId();
            ctx.getSession().lockTable(table.getId(), LockLevel.EXCLUSIVE, false);
            
            // refetch the table metadata to avoid concurrency
            table = ctx.getMetaService().getTable(trx, table.getId());
            TableMeta clone = table.clone();
        
            // new indexes blah blah
            createNewIndexes(ctx, clone, params);

            // new table
            boolean isTemporary = table.isTemproray();
            clone.setHtableId((int)ctx.getOrca().getIdentityService().getNextGlobalId(0x10));
            if (isTemporary) {
                clone.setHtableId(-clone.getHtableId());
            }
            if (humpback.getTable(table.getBlobTableId()) != null) {
                humpback.truncateTable(ctx.getHSession(), table.getBlobTableId(), clone.getBlobTableId());
            }
            humpback.truncateTable(ctx.getHSession(), table.getHtableId(), clone.getHtableId());
            meta.updateTable(ctx.getHSession(), ctx.getTransaction(), clone);
            TableMeta stub = new TableMeta(ctx.getOrca(), clone.getHtableId());
            stub.setNamespace("#");
            stub.setTableName(String.valueOf(stub.getId())  + "-" + String.valueOf(table.getId()));
            stub.setHtableId(-table.getId());
            meta.addTable(ctx.getHSession(), ctx.getTransaction(), stub);
            
            // delete old indexes
            deleteOldIndexes(ctx, table);
            
            // reset auto increment
            ObjectName seqname = table.getAutoIncrementSequenceName();
            SequenceMeta seq = meta.getSequence(trx, seqname);
            if (seq != null) {
                seq.setNextNumber(seq.getSeed());
                meta.updateSequence(ctx.getHSession(), trx.getTrxId(), seq);
            }
            
            return null;
        }
        finally {
            try {
                ctx.getSession().unlockTable(table.getId());
            }
            catch (Exception x) {
                _log.error("failed to unlock", x);
            }
        }
    }

    @Override
    List<TableMeta> getDependents() {
        return Collections.emptyList();
    }

    private void createNewIndexes(VdmContext ctx, TableMeta table, Parameters params) {
        Humpback humpback = ctx.getOrca().getHumpback();
        MetadataService meta = ctx.getMetaService();
        for (IndexMeta i:table.getIndexes()) {
            int id = (int)ctx.getOrca().getIdentityService().getNextGlobalId();
            i.setIndexTableId(id);
            if (table.isTemproray()) {
                i.setIndexTableId(-i.getIndexTableId());
            }
            i.genUniqueExternalName(table, i.getName(), id);
            humpback.createTable(
                    ctx.getHSession(),
                    table.getNamespace(),
                    i.getExternalName(),
                    i.getIndexTableId(), 
                    TableType.INDEX);
            meta.updateIndex(ctx.getHSession(), ctx.getTransaction(), i);
        }
    }

    private void deleteOldIndexes(VdmContext ctx, TableMeta table) {
        Humpback humpback = ctx.getOrca().getHumpback();
        for (IndexMeta i:table.getIndexes()) {
            try {
                humpback.dropTable(ctx.getHSession(), table.getNamespace(), i.getIndexTableId());
            }
            catch (Exception x) {
                _log.warn("failed to delete index of table: {},{}", table.toString(), i.getIndexTableId());
            }
        }
    }

}
