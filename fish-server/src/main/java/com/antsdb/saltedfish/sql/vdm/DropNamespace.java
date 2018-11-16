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

import com.antsdb.saltedfish.sql.meta.MetadataService;
import com.antsdb.saltedfish.sql.meta.TableMeta;

public class DropNamespace extends Statement {
    String name;
    boolean ifExists;
    
    public DropNamespace(String name, boolean ifExists) {
        super();
        this.name = name;
        this.ifExists = ifExists;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params) {
        // sanity checks
        
        if (ctx.getHumpback().getNamespace(this.name) == null) {
            if (this.ifExists) {
                return null;
            }
            else {
                Checks.namespaceExist(ctx.getOrca(), this.name);
            }
        }
        
        // remove the physical namespace
        
        ctx.getHumpback().dropNamespace(ctx.getHSession(), this.name);
        
        // remove all objects in the schema
        
        Transaction trx = ctx.getTransaction();
        MetadataService metaService = ctx.getMetaService();
        for (String i:metaService.getTables(trx, this.name)) {
            TableMeta table = metaService.getTable(trx, this.name, i);
            metaService.dropTable(ctx.getHSession(), trx, table);
        }
        return null;
    }

}
