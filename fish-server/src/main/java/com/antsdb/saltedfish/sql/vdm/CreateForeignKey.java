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

import java.util.ArrayList;
import java.util.List;

import com.antsdb.saltedfish.sql.LockLevel;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.ForeignKeyMeta;
import com.antsdb.saltedfish.sql.meta.IndexMeta;
import com.antsdb.saltedfish.sql.meta.RuleMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;

/**
 * instruction to create a new foriegn key
 *  
 * @author *-xguo0<@
 */
public class CreateForeignKey extends Statement {
    ObjectName childTableName;
    ObjectName parentTableName;
    List<String> childColumns;
    List<String> parentColumns;
    
    public CreateForeignKey(ObjectName childTableName, 
    		                ObjectName parentTableName,
                            List<String> childColumns, 
                            List<String> parentColumns) {
        super();
        this.childTableName = childTableName;
        this.parentTableName = parentTableName;
        this.childColumns = childColumns;
        this.parentColumns = parentColumns;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params) {
    	// find out columns from child
    	
    	List<ColumnMeta> childColumns = new ArrayList<>();
    	TableMeta child = Checks.tableExist(ctx.getSession(), childTableName);
    	for (String i:this.childColumns) {
    		ColumnMeta ii = child.getColumn(i);
    		if (ii == null) {
    			throw new OrcaException("column {} is not found on table {}", i, this.childTableName);
    		}
    		childColumns.add(ii);
    	}

    	checkParentTable(ctx, childColumns);
    	
    	ctx.getSession().lockTable(child.getId(), LockLevel.EXCLUSIVE, false);
    	try {
	    	// add the foreign key meta data
	    	
	    	Transaction trx = ctx.getTransaction();
	    	String fkname = getDefaultForeignKeyName(child);
	    	ForeignKeyMeta fk = new ForeignKeyMeta(ctx.getOrca(), child);
	    	fk.setName(fkname);
	    	StringBuilder spec = new StringBuilder();
	    	spec.append(this.parentTableName.toString());
	    	spec.append("(");
	    	for (int i=0; i<childColumns.size(); i++) {
	    		spec.append(childColumns.get(i).getColumnName());
	    		spec.append(",");
	        	fk.addColumn(ctx.getOrca(), childColumns.get(i));
	    	}
	    	spec.replace(spec.length()-1, spec.length(), ")");
	    	ctx.getMetaService().addRule(trx, fk);
	    	
	        // create index on child columns if there is none
	
	    	if (!isIndexed(child, this.childColumns)) {
	        	String indexName = getDefaultIndexName(child);
	        	CreateIndex.createIndex(ctx, child, indexName, false, childColumns);
	    	}
    	}
    	finally {
    		ctx.getSession().unlockTable(child.getId());
    	}
    	
    	return null;
    }

	private void checkParentTable(VdmContext ctx, List<ColumnMeta> childColumns) {
		if (!isForeignKeyCheckEnabled(ctx)) {
			return;
		}
		
    	// target column must be a unique key
    	
    	TableMeta parent = Checks.tableExist(ctx.getSession(), this.parentTableName);
    	RuleMeta<?> rule = null;
    	if (isMatch(parent, parent.getPrimaryKey(), this.parentColumns)) {
    		rule = parent.getPrimaryKey();
    	}
    	if (rule == null) {
    		for (IndexMeta index:parent.getIndexes()) {
    			if (index.isUnique()) {
        			if (isMatch(parent, index, this.parentColumns)) {
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
    	
    	if (parentColumns.size() != childColumns.size()) {
			throw new OrcaException("number of child columns must be the same as number of parent columns");
    	}
    	for (int i=0; i<childColumns.size(); i++) {
    		ColumnMeta columnP = parentColumns.get(i);
    		ColumnMeta columnC = childColumns.get(i);
    		if (!columnP.getDataType().equals(columnC.getDataType())) {
    			throw new OrcaException(
    					"data type of {} is not same as data type of {}", 
    					columnC.getColumnName(), 
    					columnP.getColumnName());
    		}
    	}
	}

	private boolean isForeignKeyCheckEnabled(VdmContext ctx) {
    	Object value = ctx.getSession().getVariable("FOREIGN_KEY_CHECKS");
    	if (value == null) {
    		return true;
    	}
		return Integer.valueOf(1).equals(value);
	}

	private boolean isIndexed(TableMeta table, List<String> columns) {
		if (isMatch(table, table.getPrimaryKey(), columns)) {
			return true;
		}
		for (IndexMeta i:table.getIndexes()) {
			if (isMatch(table, i, columns)) {
				return true;
			}
		}
		return false;
	}

	private boolean isMatch(TableMeta table, RuleMeta<?> rule, List<String> columns) {
		if (rule == null) {
			return false;
		}
		if (rule.getRuleColumns().size() != columns.size()) {
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

	private String getDefaultForeignKeyName(TableMeta table) {
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

}
