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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.IndexMeta;
import com.antsdb.saltedfish.sql.meta.RuleMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.sql.vdm.Checks;
import com.antsdb.saltedfish.sql.vdm.Cursor;
import com.antsdb.saltedfish.sql.vdm.CursorMeta;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.sql.vdm.ViewMaker;
import com.antsdb.saltedfish.util.CursorUtil;

public class ShowColumns extends ViewMaker {
    final static CursorMeta META_FULL = CursorUtil.toMeta(ItemFull.class);
    final static CursorMeta META_SHORT = CursorUtil.toMeta(ItemShort.class);
    
    boolean isFull = false;
    String nsName = "";
    String tableName = "";
    String like;
    private Pattern pattern;

    public static class ItemShort {
        public String Field;
        public String Type;
        public String Null;
        public String Key;
        public String Default;
        public String Extra;
    }
    
    public static class ItemFull {
        public String Field;
        public String Type;
        public String Collation;
        public String Null;
        public String Key;
        public String Default;
        public String Extra;
        public String Privileges;
        public String Comment;;
    }
    
    public ShowColumns(String ns, String table, boolean full, String like) {
        super(full ? META_FULL : META_SHORT);
        this.nsName = ns;
        this.tableName = table;
        this.isFull = full;
        this.like = like;
        if (like != null) {
            String patternText = this.like.replaceAll("_", ".");
            patternText = this.like.replaceAll("%", ".*");
            this.pattern = Pattern.compile(patternText, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        }
    }
        
    @Override
    public Object run(VdmContext ctx, Parameters notused, long pMaster) {
        
        // normalize the namespace with real name
    	
        String ns = Checks.namespaceExist(ctx.getOrca(), this.nsName);
        
        // normalize the table name
        
        TableMeta table = Checks.tableExist(ctx.getSession(), ns, this.tableName);

        if (this.isFull) {
            List<ItemFull> items = new ArrayList<>();
            for (ColumnMeta column:table.getColumns()) {
                if (!like(column)) {
                    continue;
                }
                items.add(toItemFull(table, column));
            }
            Cursor c = CursorUtil.toCursor(META_FULL, items);
            return c;
        }
        else {
            List<ItemShort> items = new ArrayList<>();
            for (ColumnMeta column:table.getColumns()) {
                if (!like(column)) {
                    continue;
                }
                items.add(toItemShort(table, column));
            }
            Cursor c = CursorUtil.toCursor(META_SHORT, items);
            return c;
        }
    }

	private boolean like(ColumnMeta column) {
	    if (pattern == null) {
	        return true;
	    }
	    Matcher m = this.pattern.matcher(column.getColumnName());
        return m.find();
    }

    private ItemShort toItemShort(TableMeta table, ColumnMeta column) {
	    ItemShort item = new ItemShort();
	    item.Field = column.getColumnName();
        item.Type = toString(column.getDataType());
	    item.Null = column.isNullable() ? "YES" : "NO";
        item.Key = getColumnKeyType(table, column);
	    item.Default = column.getDefault();
	    item.Extra = "";
        return item;
    }

    private ItemFull toItemFull(TableMeta table, ColumnMeta column) {
        ItemFull item = new ItemFull();
        item.Field = column.getColumnName();
        item.Type = toString(column.getDataType());
        item.Null = column.isNullable() ? "YES" : "NO";
        item.Key = getColumnKeyType(table, column);
        item.Default = column.getDefault();
        item.Extra = isAutoIncrement(table, column.getColumnName()) ? "auto_increment" : "";
        item.Collation = (column.getDataType().getJavaType() == String.class) ? "utf8_bin" : null;
        item.Privileges = "select,insert,update,references";
        item.Comment = "";
        return item;
    }

    private boolean contains(RuleMeta<?> rule, ColumnMeta column) {
        if (rule == null) {
            return false;
        }
        for (int columnId:rule.getRuleColumns()) {
            if (columnId == column.getId()) {
                return true;
            }
        }
        return false;
    }
    
    private String getColumnKeyType(TableMeta table, ColumnMeta column) {
        if (contains(table.getPrimaryKey(), column)) {
            return "PRI";
        }
        for (IndexMeta index:table.getIndexes()) {
            if (contains(index, column)) {
                return "MUL";
            }
        }
        return "";
    }

	private boolean isAutoIncrement(TableMeta table, String columnName) {
		ColumnMeta column = table.findAutoIncrementColumn();
		if (column == null) {
			return false;
		}
		return (column.getColumnName().equals(columnName)); 
	}
    
	private String toString(DataType type) {
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
