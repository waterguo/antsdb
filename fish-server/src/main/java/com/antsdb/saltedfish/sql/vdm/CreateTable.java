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

import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.TableType;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.meta.TableMeta;

public class CreateTable extends Statement {
    ObjectName tableName;
    private String charset;
    private String engine;
    
    public CreateTable(ObjectName tableName) {
        super();
        this.tableName = tableName;
    }

    public void setCharset(String value) {
        this.charset = value;
    }
    
    public void setEngine(String value) {
        this.engine = value;
    }
    
    @Override
    public Object run(VdmContext ctx, Parameters params) {
        Humpback humpback = ctx.getOrca().getHumpback();
        String ns = Checks.namespaceExist(ctx.getOrca(), this.tableName.getNamespace());
        if (ns.equalsIgnoreCase(Orca.SYSNS)) {
            throw new OrcaException("creating table in system database {} is not allowed", ns);
        }
        Transaction trx = ctx.getTransaction();
        
        // create metadata
        
        Checks.tableNotExist(ctx.getSession(), this.tableName);
        TableMeta tableMeta = new TableMeta(ctx.getOrca(), this.tableName);
        tableMeta.setNamespace(ns);
        tableMeta.setCharset(this.charset);
        tableMeta.setEngine(this.engine);
        ctx.getOrca().getMetaService().addTable(ctx.getHSession(), trx, tableMeta);
        
        // find a good non-conflict name
        
        tableMeta.setExternalName(tableMeta.getTableName());
        
        // create physical table

        if (Orca.SYSNS.equals(tableName.getNamespace())) {
            if (humpback.getTable(tableName.getNamespace(), tableMeta.getHtableId()) == null) {
                humpback.createTable(
                        ctx.getHSession(),
                        tableName.getNamespace(), 
                        tableMeta.getExternalName(), 
                        tableMeta.getHtableId(),
                        TableType.DATA);
            }
        }
        else {
            humpback.createTable(ctx.getHSession(),
                                 tableName.getNamespace(), 
                                 tableMeta.getExternalName(), 
                                 tableMeta.getHtableId(),
                                 TableType.DATA);
        }
        
        // done
        
        return null;
    }
}
