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

import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.TableType;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.meta.TableMeta;

public class CreateTable extends Statement {
    ObjectName tableName;
    
    public CreateTable(ObjectName tableName) {
        super();
        this.tableName = tableName;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params) {
        Humpback humpback = ctx.getOrca().getHumpback();
        String ns = Checks.namespaceExist(ctx.getOrca(), this.tableName.getNamespace());
        Transaction trx = ctx.getTransaction();
        
        // create metadata
        
        Checks.tableNotExist(ctx.getSession(), this.tableName);
        TableMeta tableMeta = new TableMeta(ctx.getOrca(), this.tableName);
        tableMeta.setNamespace(ns);
        ctx.getOrca().getMetaService().addTable(trx, tableMeta);
        
        // find a good non-conflict name
        
    	tableMeta.setExternalName(tableMeta.getTableName());
        
        // create physical table

        if (Orca.SYSNS.equals(tableName.getNamespace())) {
            if (humpback.getTable(tableName.getNamespace(), tableMeta.getHtableId()) == null) {
                humpback.createTable(
                		tableName.getNamespace(), 
                		tableMeta.getExternalName(), 
                		tableMeta.getHtableId(),
                		TableType.DATA);
            }
        }
        else {
            humpback.createTable(tableName.getNamespace(), 
            					 tableMeta.getExternalName(), 
            		             tableMeta.getHtableId(),
            		             TableType.DATA);
        }
        
        // done
        
        return null;
    }
}
