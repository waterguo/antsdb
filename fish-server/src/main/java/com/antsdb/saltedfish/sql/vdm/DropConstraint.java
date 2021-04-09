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

import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.meta.ForeignKeyMeta;
import com.antsdb.saltedfish.sql.meta.MetadataService;
import com.antsdb.saltedfish.sql.meta.TableMeta;

/**
 * 
 * @author *-xguo0<@
 */
public class DropConstraint extends Statement {
    ObjectName tableName;
    String name;
    
    public DropConstraint(ObjectName table, String name) {
        super();
        this.tableName = table;
        this.name = name;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params) {
        TableMeta table = Checks.tableExist(ctx.getSession(), this.tableName);
        ForeignKeyMeta found = null;
        for (ForeignKeyMeta i:table.getForeignKeys()) {
            if (i.getName().equalsIgnoreCase(this.name)) {
                found = i;
                break;
            }
        }
        if (found == null) {
            throw new OrcaException("constraint {} is not found", this.name); 
        }
        MetadataService meta = ctx.getMetaService();
        meta.deleteRule(ctx.getHSession(), ctx.getTransaction(), found);
        return null;
    }
}
