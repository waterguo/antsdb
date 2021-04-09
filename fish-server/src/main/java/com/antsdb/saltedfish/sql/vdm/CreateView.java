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

import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.meta.OrcaTableType;
import com.antsdb.saltedfish.sql.meta.TableMeta;

/**
 * 
 * @author *-xguo0<@
 */
public class CreateView extends Statement {

    private ObjectName name;
    private String sql;

    public CreateView(ObjectName name, String sql) {
        this.name = name;
        this.sql = sql;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params) {
        String ns = Checks.namespaceExist(ctx.getOrca(), this.name.getNamespace());
        if (ns.equalsIgnoreCase(Orca.SYSNS)) {
            throw new OrcaException("creating table in system database {} is not allowed", ns);
        }
        Transaction trx = ctx.getTransaction();
        
        // create metadata
        Checks.tableNotExist(ctx.getSession(), this.name);
        TableMeta tableMeta = new TableMeta(ctx.getOrca(), this.name);
        tableMeta.setNamespace(ns);
        tableMeta.setType(OrcaTableType.VIEW);
        tableMeta.setViewSql(this.sql);
        ctx.getOrca().getMetaService().addTable(ctx.getHSession(), trx, tableMeta);
        return null;
    }
}
