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

import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.PrimaryKeyMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.sql.planner.SortKey;

public class TableScan extends CursorMaker implements Ordered {
    HumpbackTableScan upstream;
    TableMeta table;
    
    public TableScan(TableMeta table, int makerId) {
    	    this.table = table;
        this.upstream = new HumpbackTableScan(table, makerId);
    }

    @Override
    public CursorMeta getCursorMeta() {
        return this.upstream.getCursorMeta();
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        return this.upstream.run(ctx, params, pMaster);
    }

    @Override
    public String toString() {
        return "Table Scan (" + this.table.getObjectName() + ")";
    }

    @Override
    public void explain(int level, List<ExplainRecord> records) {
    	    this.upstream.explain(level, records);
    }

    @Override
    public List<ColumnMeta> getOrder() {
        PrimaryKeyMeta key = this.table.getPrimaryKey();
        if (key == null) {
            return Collections.emptyList();
        }
        return this.table.getPrimaryKey().getColumns(table);
    }

    @Override
    public boolean setSortingOrder(List<SortKey> order) {
        return this.upstream.setSortingOrder(order);
    }

    public void setNoCache(boolean value) {
        this.upstream.setNoCache(value);
    }

}
