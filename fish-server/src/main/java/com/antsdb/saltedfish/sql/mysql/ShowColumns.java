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
package com.antsdb.saltedfish.sql.mysql;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.RuleMeta;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.DataTypeFactory;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.sql.vdm.Checks;
import com.antsdb.saltedfish.sql.vdm.Cursor;
import com.antsdb.saltedfish.sql.vdm.FieldMeta;
import com.antsdb.saltedfish.sql.vdm.CursorMaker;
import com.antsdb.saltedfish.sql.vdm.CursorMeta;
import com.antsdb.saltedfish.sql.vdm.HashMapRecord;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.Record;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.util.CursorUtil;
import com.antsdb.saltedfish.util.UberUtil;

public class ShowColumns extends CursorMaker {

    boolean isFull = false;
    String nsName = "";
    String tableName = "";
    String like;

    public ShowColumns(String ns, String table, boolean full, String like) {
        this.nsName = ns;
        this.tableName = table;
        this.isFull = full;
        this.like = like;
    }
        
    @Override
    public CursorMeta getCursorMeta() {
        return null;
    }
    
    @Override
    public Object run(VdmContext ctx, Parameters notused, long pMaster) {
        
        // normalize the namespace with real name
    	
        String ns = Checks.namespaceExist(ctx.getOrca(), this.nsName);
        
        // normalize the table name
        
        TableMeta table = Checks.tableExist(ctx.getSession(), ns, this.tableName);

        // Read in meta from __SYS.SYSCOLUMN
        
        String sql = "SELECT c.id, column_name, type_name, type_length, type_scale, nullable, default_value, r.rule_type" 
                   + " FROM __sys.syscolumn c"
        		   + " LEFT JOIN __sys.sysrulecol rc ON (c.id=rc.column_id)"
        		   + " LEFT JOIN __sys.sysrule r ON (rc.rule_id=r.id)"
        		   + " WHERE c.namespace=? and c.table_name=?";
        if (this.like != null) {
        	sql += " AND column_name LIKE ?"; 
        }
        Parameters params;
        if (this.like != null) {
        	params = new Parameters(new Object[]{table.getNamespace(), table.getTableName(), this.like});
        } 
        else {
        	params = new Parameters(new Object[]{table.getNamespace(), table.getTableName()});
        }

        // make cursor meta
        
        CursorMeta meta = new CursorMeta();
        if (this.isFull) {
            meta.addColumn(new FieldMeta("Field", DataType.varchar()))
                .addColumn(new FieldMeta("Type", DataType.varchar()))
                .addColumn(new FieldMeta("Collation", DataType.varchar()))
                .addColumn(new FieldMeta("Null", DataType.varchar()))
                .addColumn(new FieldMeta("Key", DataType.varchar()))
                .addColumn(new FieldMeta("Default", DataType.varchar()))
                .addColumn(new FieldMeta("Extra", DataType.varchar()))
        	    .addColumn(new FieldMeta("Privileges", DataType.varchar()))
    	        .addColumn(new FieldMeta("Comment", DataType.varchar()));
        }
        else {
            meta.addColumn(new FieldMeta("Field", DataType.varchar()))
                .addColumn(new FieldMeta("Type", DataType.varchar()))
                .addColumn(new FieldMeta("Null", DataType.varchar()))
                .addColumn(new FieldMeta("Key", DataType.varchar()))
                .addColumn(new FieldMeta("Default", DataType.varchar()))
                .addColumn(new FieldMeta("Extra", DataType.varchar()));
        }
        
        // generate result
        
        DataTypeFactory fac = ctx.getOrca().getTypeFactory();
        List<Record> records = new ArrayList<>();
        try (Cursor c = (Cursor)(ctx.getSession().parse(sql).run(ctx, params, 0));) {
        	for (long pRecord = c.next(); pRecord != 0; pRecord = c.next()) {
	        	String columnName = (String)Record.getValue(pRecord, 1);
	        	String typeName = (String)Record.getValue(pRecord, 2);
	        	Integer typeLength = (Integer)Record.getValue(pRecord, 3);
	        	Integer typeScale =  (Integer)Record.getValue(pRecord, 4);
	        	Boolean nullable = (Boolean)Record.getValue(pRecord, 5);
	        	String defaultValue = (String)Record.getValue(pRecord, 6);
	        	Integer ruleType = (Integer)Record.getValue(pRecord, 7);
	        	DataType type = fac.newDataType(typeName, typeLength, typeScale);
	        	Record rec = new HashMapRecord();
	        	if (this.isFull) {
	            	rec.set(0, columnName)
	      	           .set(1, toString(type))
	      	           .set(2, type.getJavaType() == String.class ? "utf8_bin" : null)
	      	           .set(3, nullable ? "YES" : "NO")
	      	           .set(4, getKeyType(ruleType))
	      	           .set(5, defaultValue)
	      	           .set(6, isAutoIncrement(table, columnName) ? "auto_increment" : "")
	      	           .set(7, "select,insert,update,references")
	      	           .set(8, "");
	        	}
	        	else {
	            	rec.set(0, columnName)
	         	       .set(1, toString(type))
	         	       .set(2, nullable ? "YES" : "NO")
	         	       .set(3, UberUtil.safeEqual(ruleType, RuleMeta.Rule.PrimaryKey.ordinal()) ? "PRI" : "")
	         	       .set(4, defaultValue)
	         	       .set(5, "");
	        	}
	        	records.add(rec);
        	}
        }
        
        return CursorUtil.toCursor(meta, records);
    }

	private String getKeyType(Integer ruleType) {
		if (ruleType == null) {
			return "";
		}
		if (RuleMeta.Rule.PrimaryKey.ordinal() == ruleType) {
			return "PRI";
		}
		else if (RuleMeta.Rule.Index.ordinal() == ruleType) {
			return "MUL";
		}
		else {
			return ruleType.toString();
		}
	}

	private boolean isAutoIncrement(TableMeta table, String columnName) {
		ColumnMeta column = table.findAutoIncrementColumn();
		if (column == null) {
			return false;
		}
		return (column.getColumnName().equals(columnName)); 
	}

	private Object toString(DataType type) {
		if (type.getJavaType() == BigDecimal.class) {
			return type.toString();
		}
		else if (type.getLength() > 0) {
			return type.getName() + "(" + type.getLength() + ")";
		}
		else {
			return type.toString();
		}
	}
}
