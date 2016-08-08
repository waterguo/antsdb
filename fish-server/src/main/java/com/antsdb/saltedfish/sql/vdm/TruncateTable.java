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

import java.util.Collections;
import java.util.List;

import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.sql.LockLevel;
import com.antsdb.saltedfish.sql.meta.IndexMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;

public class TruncateTable extends Statement {
    ObjectName tableName;

    public TruncateTable(ObjectName tableName) {
        super();
        this.tableName = tableName;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params) {
        TableMeta table = Checks.tableExist(ctx.getSession(), this.tableName);
    	try {
            // acquire exclusive lock
            
    		ctx.getSession().lockTable(table.getId(), LockLevel.EXCLUSIVE, false);
            
            // indexes blah blah
            
            truncateIndexes(ctx, table, params);
            
            // truncate physical table
            
            GTable gtable = ctx.getHumpback().getTable(this.tableName.getNamespace(), table.getId());
            gtable.truncate();
            return null;
    	}
    	finally {
    		ctx.getSession().unlockTable(table.getId());
    	}
    }

    @Override
    List<TableMeta> getDependents() {
        return Collections.emptyList();
    }

	private void truncateIndexes(VdmContext ctx, TableMeta table, Parameters params) {
        Humpback humpback = ctx.getOrca().getStroageEngine();
		for (IndexMeta i:table.getIndexes()) {
			GTable gindex = humpback.getTable(i.getIndexTableId());
			gindex.truncate();
		}
	}

}
