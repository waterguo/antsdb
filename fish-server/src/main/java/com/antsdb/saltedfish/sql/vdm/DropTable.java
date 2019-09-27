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
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;

import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.SysMetaRow;
import com.antsdb.saltedfish.sql.LockLevel;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.meta.ForeignKeyMeta;
import com.antsdb.saltedfish.sql.meta.IndexMeta;
import com.antsdb.saltedfish.sql.meta.MetadataService;
import com.antsdb.saltedfish.sql.meta.SequenceMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.util.UberUtil;

public class DropTable extends Statement {
    static Logger _log = UberUtil.getThisLogger();
    
    List<ObjectName> tableNames;
    boolean ifExist;
    
    public DropTable(ObjectName table, boolean ifExist) {
        this(Collections.singletonList(table), ifExist);
    }
    
    public DropTable(List<ObjectName> tables, boolean ifExist) {
        super();
        this.tableNames = tables;
        this.ifExist = ifExist;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params) {
        List<ObjectName> failTbls = new ArrayList<>();
        for (ObjectName tblName: tableNames)
        {
            if (StringUtils.isEmpty(tblName.getNamespace())) {
                throw new OrcaException("No database selected");
            }
            TableMeta table = ctx.getSession().getTable(tblName);
            if (table == null) {
                if (!this.ifExist) {
                    failTbls.add(tblName);
                }
                continue;
            }

            try {
                // acquire exclusive lock
                Transaction trx = ctx.getTransaction();
                ctx.getSession().lockTable(table.getId(), LockLevel.EXCLUSIVE, false);
                
                // refetch the table metadata to avoid concurrency
                table = ctx.getMetaService().getTable(trx, table.getId());
                check(ctx, table);
                
                // indexes blah blah
                dropDependents(ctx, table, params);
                
                // drop physical table
                Humpback humpback = ctx.getOrca().getHumpback();
                humpback.dropTable(ctx.getHSession(), tblName.getNamespace(), table.getHtableId());
                if (humpback.getTable(table.getBlobTableId()) != null) {
                    humpback.dropTable(ctx.getHSession(), tblName.getNamespace(), table.getBlobTableId());
                }
                
                // remove metadata
                MetadataService meta = ctx.getOrca().getMetaService();
                if (table.findAutoIncrementColumn() != null) {
                    SequenceMeta seq = meta.getSequence(trx, table.getAutoIncrementSequenceName());
                    if (seq != null) {
                        meta.dropSequence(ctx.getHSession(), trx, seq);
                    }
                }
                ctx.getOrca().getMetaService().dropTable(ctx.getHSession(), trx, table);
                if (table.isTemproray()) {
                    ctx.getSession().removeTemporaryTable(table);
                }
            }
            finally {
                ctx.getSession().unlockTable(table.getId());
            }
        }
        if (failTbls.size()>0) {
            throw new OrcaException(failTbls.toString() + "table doesn't exist");
        };
        return null;
    }

    private void check(VdmContext ctx, TableMeta table) {
        Humpback humpback = ctx.getHumpback();
        if (table.getHtableId() >= 0) {
            SysMetaRow meta = humpback.getTableInfo(table.getHtableId());
            if (meta != null) {
                if (!meta.isDeleted()) {
                    return;
                }
            }
            _log.warn("table {} is out of sync between orca and humpback", table.getId());
        }
    }

    private void dropDependents(VdmContext ctx, TableMeta table, Parameters params) {
        for (IndexMeta i:table.getIndexes()) {
            DropIndex drop = new DropIndex(table.getObjectName(), i.getName());
            drop.run(ctx, params);
        }
        for (ForeignKeyMeta i:table.getForeignKeys()) {
            DropForeignKey drop = new DropForeignKey(table.getObjectName(), i.getName());
            drop.run(ctx, params);
        }
    }

}
