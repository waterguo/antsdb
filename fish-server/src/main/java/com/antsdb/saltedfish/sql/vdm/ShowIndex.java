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

import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.IndexMeta;
import com.antsdb.saltedfish.sql.meta.PrimaryKeyMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.util.CursorUtil;

public class ShowIndex extends ViewMaker {
	final static CursorMeta META = CursorUtil.toMeta(Item.class);

	ObjectName tableName;
	
	public static class Item {
		public String Table;
		public int Non_unique;
		public String Key_name;
		public int Seq_in_index;
		public String Column_name;
		public String Collation;
		public long Cardinality;
		public Integer Sub_part;
		public String Packed;
		public String Null;
		public String Index_type;
		public String Comment;
		public String Index_comment;
	}
	
	public ShowIndex(ObjectName tableName, GeneratorContext ctx) {
	    super(META);
		this.tableName = tableName;
	}

	@Override
	public Object run(VdmContext ctx, Parameters params, long pMaster) {
		TableMeta table = Checks.tableExist(ctx.getSession(), this.tableName);
		List<Item> list = new ArrayList<>();
		toItems(list, table, table.getPrimaryKey());
		for (IndexMeta index: table.getIndexes()) {
	        toItems(list, table, index);
		}
        Cursor c = CursorUtil.toCursor(META, list);
        return c;
	}

    private void toItems(List<Item> items, TableMeta table, IndexMeta index) {
        int sequence = 1;
        for (ColumnMeta column:index.getColumns(table)) {
            Item item = new Item();
            item.Table = table.getTableName();
            item.Non_unique = index.isUnique() ? 0 : 1;
            item.Key_name = index.getName();
            item.Seq_in_index = sequence++;
            item.Column_name = column.getColumnName();
            item.Collation = "A";
            item.Cardinality = 0;
            item.Null = "YES";
            item.Index_type = "BTREE";
            item.Comment = "";
            item.Index_comment = "";
            items.add(item);
        }
    }

    private void toItems(List<Item> items, TableMeta table, PrimaryKeyMeta primaryKey) {
        int sequence = 1;
        for (ColumnMeta column:primaryKey.getColumns(table)) {
            Item item = new Item();
            item.Table = table.getTableName();
            item.Non_unique = 0;
            item.Key_name = "PRIMARY";
            item.Seq_in_index = sequence++;
            item.Column_name = column.getColumnName();
            item.Collation = "A";
            item.Cardinality = 0;
            item.Null = "";
            item.Index_type = "BTREE";
            item.Comment = "";
            item.Index_comment = "";
            items.add(item);
        }
    }
}
