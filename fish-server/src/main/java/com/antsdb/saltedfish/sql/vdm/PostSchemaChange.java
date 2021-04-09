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

import java.util.List;

import org.slf4j.Logger;

import com.antsdb.saltedfish.nosql.HColumnRow;
import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.SysMetaRow;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * inform the   
 * @author *-xguo0<@
 */
public class PostSchemaChange extends Statement {
    private static final Logger _log = UberUtil.getThisLogger();

    private ObjectName tableName;

    public PostSchemaChange(ObjectName tableName) {
        this.tableName = tableName;
    }
    
    @Override
    public Object run(VdmContext ctx, Parameters params) {
        try {
            TableMeta table = Checks.tableExist(ctx.getSession(), this.tableName);
            Humpback humpback = ctx.getHumpback();
            SysMetaRow tableMeta = humpback.getTableInfo(table.getId());
            List<HColumnRow> columns = humpback.getColumns(table.getId());
            humpback.getStorageEngine().postSchemaChange(tableMeta, columns);
            if (humpback.getTable(table.getBlobTableId()) != null) {
                tableMeta = humpback.getTableInfo(table.getBlobTableId());
                columns = humpback.getColumns(table.getBlobTableId());
                humpback.getStorageEngine().postSchemaChange(tableMeta, columns);
            }
        }
        catch (Exception x) {
            _log.error("error", x);
        }
        return null;
    }

}
