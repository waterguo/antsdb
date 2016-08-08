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
package com.antsdb.saltedfish.sql;

import java.util.List;

import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.sql.planner.Planner;
import com.antsdb.saltedfish.sql.vdm.CursorMaker;
import com.antsdb.saltedfish.sql.vdm.Delete;
import com.antsdb.saltedfish.sql.vdm.DeleteSingleRow;
import com.antsdb.saltedfish.sql.vdm.Operator;
import com.antsdb.saltedfish.sql.vdm.Statement;
import com.antsdb.saltedfish.sql.vdm.TableSeek;
import com.antsdb.saltedfish.sql.vdm.Update;
import com.antsdb.saltedfish.sql.vdm.UpdateSingleRow;

public class GeneratorUtil {

    public static Statement genDelete(GeneratorContext ctx, TableMeta table, Planner planner) {
        CursorMaker maker = planner.run();
        GTable gtable = ctx.getGtable(table.getObjectName());
        if (maker instanceof TableSeek) {
            TableSeek seeker = (TableSeek)maker;
            return new DeleteSingleRow(ctx.getOrca(), table, gtable, seeker.getKey());
        }
        else {
            return new Delete(ctx.getOrca(), table, gtable, maker);
        }
    }
    
    public static Statement genUpdate(
            GeneratorContext ctx, 
            TableMeta table, 
            CursorMaker maker, 
            List<ColumnMeta> columns, 
            List<Operator> exprs) {
        GTable gtable = ctx.getGtable(table.getObjectName());
        if (maker instanceof TableSeek) {
            TableSeek seeker = (TableSeek)maker;
            return new UpdateSingleRow(ctx.getOrca(), table, gtable, seeker.getKey(), columns, exprs);
        }
        else {
            return new Update(ctx.getOrca(), table, gtable, maker, columns, exprs);
        }
    }
    
}
