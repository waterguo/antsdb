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

import java.util.List;

import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.sql.LockLevel;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.PrimaryKeyMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;

public class CreatePrimaryKey extends Statement {
    ObjectName name;
    List<String> columns;
    
    public CreatePrimaryKey(ObjectName name, List<String> columns) {
        super();
        this.name = name;
        this.columns = columns;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params) {
        // lock the table exclusively
        
        TableMeta table = Checks.tableExist(ctx.getSession(), name);
        try {
        	ctx.getSession().lockTable(table.getId(), LockLevel.EXCLUSIVE, false);
            // make the meta data change
            
            create(ctx, params);
            return null;
        }
        finally {
        	ctx.getSession().unlockTable(table.getId());
        }
    }

    private void create(VdmContext ctx, Parameters params) {
        
        // table must be empty
        
        TableMeta table = Checks.tableExist(ctx.getSession(), this.name);
        ensureTableEmpty(ctx, table);
        if (table.getPrimaryKey() != null) {
            throw new OrcaException("primary key already exists");
        }
        
        // create the primary key
        
        PrimaryKeyMeta pk = new PrimaryKeyMeta(ctx.getOrca(), table);
        List<ColumnMeta> ruleColumns = table.getColumnsByName(this.columns);
        pk.setRuleColumns(ruleColumns);
        ctx.getMetaService().addRule(ctx.getTransaction(), pk);
    }

    private void ensureTableEmpty(VdmContext ctx, TableMeta tableMeta) {
        GTable table = ctx.getHumpback().getTable(name.getNamespace(), tableMeta.getHtableId());
        if (!table.isPureEmpty()) {
            throw new OrcaException("table must be empty when adding primary key");
        }
    }

}
