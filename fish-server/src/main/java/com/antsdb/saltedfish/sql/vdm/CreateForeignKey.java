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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.antsdb.saltedfish.sql.LockLevel;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.ForeignKeyMeta;
import com.antsdb.saltedfish.sql.meta.IndexMeta;
import com.antsdb.saltedfish.sql.meta.MetadataService;
import com.antsdb.saltedfish.sql.meta.PrimaryKeyMeta;
import com.antsdb.saltedfish.sql.meta.RuleMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;

/**
 * instruction to create a new foreign key
 *  
 * @author *-xguo0<@
 */
public class CreateForeignKey extends Statement {
    static List<ForeignKeySpec> _delayed = Collections.synchronizedList(new ArrayList<>());
    
    ForeignKeySpec spec = new ForeignKeySpec();
    private boolean rebuildIndex = true;
    
    static class ForeignKeySpec {
        String name;
        ObjectName childTableName;
        ObjectName parentTableName;
        List<String> childColumns;
        List<String> parentColumns;
        public String onDelete;
        public String onUpdate;
    }
    
    public CreateForeignKey(ObjectName childTableName, 
                            String name, 
                            ObjectName parentTableName,
                            List<String> childColumns, 
                            List<String> parentColumns,
                            String onDeleteAction,
                            String onUpdateAction) {
        super();
        this.spec.name = name;
        this.spec.childTableName = childTableName;
        this.spec.parentTableName = parentTableName;
        this.spec.childColumns = childColumns;
        this.spec.parentColumns = parentColumns;
        this.spec.onDelete = onDeleteAction;
        this.spec.onUpdate = onUpdateAction;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params) {
        // find out columns from child

        TableMeta child = Checks.tableExist(ctx.getSession(), this.spec.childTableName);
        List<ColumnMeta> childColumns = getColumns(child, spec.childColumns);
        ctx.getSession().lockTable(child.getId(), LockLevel.EXCLUSIVE, false);
        try {
            // FOREIGN_KEY_CHECKS will make the logic go on even if there are errors
            
            try {
                checkParent(ctx, spec);
            }
            catch (OrcaException x) {
                if (!"0".equals(ctx.getSession().getConfig().get("FOREIGN_KEY_CHECKS"))) {
                    throw x;
                }
            }
            
            // create the foreign key metadata
            
            createMeta(ctx, child, this.spec);

            // create index on child columns if there is none

            if (!isIndexed(child, this.spec.childColumns)) {
                String indexName = getDefaultIndexName(child);
                CreateIndex.createIndex(ctx, child, indexName, false, false, childColumns, this.rebuildIndex);
            }
        }
        finally {
            ctx.getSession().unlockTable(child.getId());
        }

        return null;
    }

    private static List<ColumnMeta> getColumns(TableMeta table, List<String> names) {
        ArrayList<ColumnMeta> result = new ArrayList<>();
        for (String i:names) {
            ColumnMeta ii = table.getColumn(i);
            if (ii == null) {
                throw new OrcaException("column {} is not found on table {}", i, table.getObjectName());
            }
            result.add(ii);
        }
        return result;
    }
    
    private static boolean checkParent(VdmContext ctx, ForeignKeySpec spec) {
        // check table name
        
        MetadataService meta = ctx.getMetaService();
        TableMeta table = meta.getTable(ctx.getTransaction(), spec.parentTableName);
        if (table == null) {
            throw new OrcaException("parent table {} is not found", spec.parentTableName);
        }
        spec.parentTableName = table.getObjectName();
        
        // check column name
        
        for (int i=0; i<spec.parentColumns.size(); i++) {
            ColumnMeta ii = table.getColumn(spec.parentColumns.get(i));
            if (ii == null) {
                throw new OrcaException("parent column {}.{} is not found", 
                                        spec.parentTableName, 
                                        spec.parentColumns.get(i)) ;
            }
            spec.parentColumns.set(i, ii.getColumnName());
        }
        
        // check key
        
        for (;;) {
            PrimaryKeyMeta pk = table.getPrimaryKey();
            if (pk != null) {
                if (pk.conform(table, spec.parentColumns)) {
                    break;
                }
            }
            for (IndexMeta i:table.getIndexes()) {
                if (!i.isUnique()) {
                    continue;
                }
                if (i.conform(table, spec.parentColumns)) {
                    break;
                }
            }
            throw new OrcaException("parent column {}({}) is neither a primary key or unique key", 
                                    spec.parentTableName,
                                    StringUtils.join(spec.parentColumns, ","));
        }
        return true;
    }
    
    private static void createMeta(VdmContext ctx, TableMeta child, ForeignKeySpec spec) {
        Transaction trx = ctx.getTransaction();
        String fkname = (spec.name != null) ? spec.name : getDefaultForeignKeyName(child);
        ForeignKeyMeta fk = new ForeignKeyMeta(ctx.getOrca(), child);
        fk.setNamespace(child.getNamespace());
        fk.setName(fkname);
        fk.setRuleColumns(child.getColumnsByName(spec.childColumns));
        fk.setParentTable(spec.parentTableName);
        fk.setParentColumns(spec.parentColumns);
        fk.setOnDelete(spec.onDelete);
        fk.setOnUpdate(spec.onUpdate);
        ctx.getMetaService().addRule(ctx.getHSession(), trx, fk);
    }

    private boolean isIndexed(TableMeta table, List<String> columns) {
        if (doesMatch(table, table.getPrimaryKey(), columns)) {
            return true;
        }
        for (IndexMeta i:table.getIndexes()) {
            if (doesMatch(table, i, columns)) {
                return true;
            }
        }
        return false;
    }

    private static boolean doesMatch(TableMeta table, RuleMeta<?> rule, List<String> columns) {
        if (rule == null) {
            return false;
        }
        if (rule.getRuleColumns().length != columns.size()) {
            return false;
        }
        int idx = 0;
        for (ColumnMeta i:rule.getColumns(table)) {
            if (!i.getColumnName().equalsIgnoreCase(columns.get(idx++))) {
                return false;
            }
        }
        return true;
    }
    
    private String getDefaultIndexName(TableMeta table) {
        for (int i=1; true; i++) {
            String name = table.getTableName() + "_idx_" + i;
            boolean found = false;
            for (IndexMeta fk:table.getIndexes()) {
                if (fk.getName().equalsIgnoreCase(name)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                return name;
            }
        }
    }

    private static String getDefaultForeignKeyName(TableMeta table) {
        for (int i=1; true; i++) {
            String name = table.getTableName() + "_fk_" + i;
            boolean found = false;
            for (ForeignKeyMeta fk:table.getForeignKeys()) {
                if (fk.getName().equalsIgnoreCase(name)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                return name;
            }
        }
    }

    public static void deleteDelayedKeys(ObjectName name) {
        for (Iterator<ForeignKeySpec> it = _delayed.iterator(); it.hasNext();) {
            ForeignKeySpec i = it.next();
            if (i.childTableName.equals(name)) {
                it.remove();
            }
        }
    }
    
    public void setRebuildIndex(boolean value) {
        this.rebuildIndex = value;
    }
}
