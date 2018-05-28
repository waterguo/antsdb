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
import com.antsdb.saltedfish.sql.meta.IndexMeta;
import com.antsdb.saltedfish.sql.meta.MetadataService;
import com.antsdb.saltedfish.sql.meta.TableMeta;

public class DropIndex extends Statement {
    ObjectName tableName;
    String indexName;
    
    public DropIndex(ObjectName table, String index) {
        super();
        this.tableName = table;
        this.indexName = index;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params) {
        TableMeta table = Checks.tableExist(ctx.getSession(), this.tableName);
        IndexMeta index = Checks.indexExist(table, this.indexName);
        
        // drop physical index
        
        Humpback humpback = ctx.getOrca().getHumpback();
        humpback.dropTable(this.tableName.getNamespace(), index.getIndexTableId());

        // drop metadata
        
        MetadataService meta = ctx.getMetaService();
        meta.deleteRule(ctx.getTransaction(), index);

        return null;
    }

}
