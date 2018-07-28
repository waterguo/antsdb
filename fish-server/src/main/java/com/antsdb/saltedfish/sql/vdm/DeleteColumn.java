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

import java.util.ArrayList;
import java.util.List;

import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.IndexMeta;
import com.antsdb.saltedfish.sql.meta.PrimaryKeyMeta;
import com.antsdb.saltedfish.sql.meta.RuleMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;

public class DeleteColumn extends Statement {
    ObjectName tableName;
    String columnName;
    
    public DeleteColumn(ObjectName tableName, String columnName) {
        super();
        this.tableName = tableName;
        this.columnName = columnName;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params) {
        TableMeta table = Checks.tableExist(ctx.session, this.tableName);
        ColumnMeta column = Checks.columnExist(table, this.columnName);
        Transaction trx = ctx.getTransaction();

        // scan for affected indexes. 
        
        GTable gtable = ctx.getHumpback().getTable(table.getHtableId());
        for (IndexMeta index:table.getIndexes()) {
            List<ColumnMeta> indexColumns = index.getColumns(table);
            if (indexColumns.contains(column)) {
                if (!gtable.isPureEmpty()) {
                    throw new OrcaException("table must be empty in order to drop a index column");
                }
                if (indexColumns.size() == 1) {
                    // remove the index
                    new DropIndex(this.tableName, index.getName()).run(ctx, params);
                }
                else {
                    // remove the index column
                    indexColumns.remove(column);
                    index.setRuleColumns(indexColumns);
                    ctx.getMetaService().updateRule(trx, index);
                }
            }
        }
        
        // check primary key 

        if (table.getPrimaryKey() != null) {
            PrimaryKeyMeta pk = table.getPrimaryKey();
            List<ColumnMeta> indexColumns = pk.getColumns(table);
            if (indexColumns.contains(column)) {
                if (!gtable.isPureEmpty()) {
                    throw new OrcaException("table must be empty in order to drop a primary key column");
                }
                if (indexColumns.size() == 1) {
                    // remove the index
                    ctx.getMetaService().deleteRule(trx, pk);
                }
                else {
                    // remove the index column
                    indexColumns.remove(column);
                    pk.setRuleColumns(indexColumns);
                    ctx.getMetaService().updateRule(trx, pk);
                }
            }
        }

        // remove column from meta-data
        
        ctx.getMetaService().deleteColumn(trx, table, column);
        
        return null;
    }
    
    List<String> deleteColumnFromRule(TableMeta table, RuleMeta<?> rule, ColumnMeta column) {
        List<String> newColumns = new ArrayList<>();
        List<ColumnMeta> columns = rule.getColumns(table);
        if (!columns.contains(column)) {
            return null;
        }
        for (ColumnMeta i:columns) {
            if (i == column) {
                continue;
            }
            newColumns.add(i.getColumnName());
        }
        return newColumns;
    }
}
