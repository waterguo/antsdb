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

import com.antsdb.saltedfish.sql.LockLevel;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.ForeignKeyMeta;
import com.antsdb.saltedfish.sql.meta.IndexMeta;
import com.antsdb.saltedfish.sql.meta.MetadataService;
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
    }
    
    public CreateForeignKey(ObjectName childTableName, 
    		                String name, 
    		                ObjectName parentTableName,
                            List<String> childColumns, 
                            List<String> parentColumns) {
        super();
        this.spec.name = name;
        this.spec.childTableName = childTableName;
        this.spec.parentTableName = parentTableName;
        this.spec.childColumns = childColumns;
        this.spec.parentColumns = parentColumns;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params) {
        // find out columns from child
    	
    	    TableMeta child = Checks.tableExist(ctx.getSession(), this.spec.childTableName);
        List<ColumnMeta> childColumns = getColumns(child, spec.childColumns);
    	    ctx.getSession().lockTable(child.getId(), LockLevel.EXCLUSIVE, false);
    	    try {
    	    	// add the foreign key meta data
    	    	
        	    TableMeta parent = findParent(ctx, spec.parentTableName);
        	    if (parent != null) {
                createMeta(ctx, parent, child, this.spec);
        	    } 
        	    else {
                _delayed.add(this.spec);
        	    }
    	    	
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
    
	private TableMeta findParent(VdmContext ctx, ObjectName name) {
	    MetadataService meta = ctx.getMetaService();
        TableMeta table = meta.getTable(ctx.getTransaction(), name);
        if (table == null) {
            if (ctx.getSession().getConfig().getForeignKeyCheck()) {
                throw new OrcaException("table {} is not found", name);
            }
        }
        return table;
    }

    private static void createMeta(VdmContext ctx, TableMeta parent, TableMeta child, ForeignKeySpec spec) {
        checkParentTable(ctx, parent, child, spec);
        Transaction trx = ctx.getTransaction();
        String fkname = (spec.name != null) ? spec.name : getDefaultForeignKeyName(child);
        ForeignKeyMeta fk = new ForeignKeyMeta(ctx.getOrca(), child);
        fk.setNamespace(child.getNamespace());
        fk.setName(fkname);
        fk.setRuleColumns(child.getColumnsByName(spec.childColumns));
        fk.setParentTable(parent.getId());
        fk.setRuleParentColumns(parent.getColumnsByName(spec.parentColumns));
        ctx.getMetaService().addRule(trx, fk);
    }

    private static boolean checkParentTable(VdmContext ctx, TableMeta parent, TableMeta child, ForeignKeySpec spec) {
        // target column must be a unique key

        RuleMeta<?> rule = null;
        if (doesMatch(parent, parent.getPrimaryKey(), spec.parentColumns)) {
            rule = parent.getPrimaryKey();
        }
        if (rule == null) {
            for (IndexMeta index : parent.getIndexes()) {
                if (index.isUnique()) {
                    if (doesMatch(parent, index, spec.parentColumns)) {
                        rule = index;
                        break;
                    }
                }
            }
        }
        if (rule == null) {
            throw new OrcaException("parent columns are neither primary key or unique key");
        }

        // find out columns from parent

        List<ColumnMeta> parentColumns = rule.getColumns(parent);

        // parent columns and child columns must match in terms of data type

        List<ColumnMeta> childColumns = getColumns(child, spec.childColumns);
        if (parentColumns.size() != childColumns.size()) {
            throw new OrcaException("number of child columns must be the same as number of parent columns");
        }
        for (int i = 0; i < childColumns.size(); i++) {
            ColumnMeta columnP = parentColumns.get(i);
            ColumnMeta columnC = childColumns.get(i);
            if (!columnP.getDataType().equals(columnC.getDataType())) {
                throw new OrcaException("data type of {} is not same as data type of {}", columnC.getColumnName(),
                        columnP.getColumnName());
            }
        }
        return true;
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
	
	public static void createDelayedKeys(VdmContext ctx, TableMeta table) {
	    // find the foreign key spec
	    
	    ForeignKeySpec found = null;
	    for (ForeignKeySpec i:_delayed) {
	        if (i.parentTableName.equals(table.getObjectName())) {
	            found = i;
	            break;
	        }
	    }
	    if (found == null) {
	        return;
	    }
	    
	    // find the child table
	    
	    MetadataService meta = ctx.getMetaService();
	    TableMeta child = meta.getTable(ctx.getTransaction(), found.childTableName);
	    if (child == null) {
	        // child table is probably deleted
	        _delayed.remove(found);
	        return;
	    }
	    
	    // create foreign key metadata
	    
	    createMeta(ctx, table, child, found);
        _delayed.remove(found);
	}

    public void setRebuildIndex(boolean value) {
        this.rebuildIndex = value;
    }
}
