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

import java.util.List;

import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.meta.RuleMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.util.CursorUtil;

public class ShowIndex extends CursorMaker {
	final static CursorMeta META = CursorUtil.toMeta(Item.class);

	ObjectName tableName;

	private Script script;
	
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
	
	private static class Transformer extends CursorWithHeap {
		Cursor upstream;
		int sequence = 1;
		int lastRuleId = -1;
		
		Transformer(Cursor upstream) {
			super(ShowIndex.META);
			this.upstream = upstream;
		}

		@Override
		public long next() {
			long pRec = this.upstream.next();
			if (pRec == 0) {
				return 0;
			}
			Item item = new Item();
			Boolean isIndexUnique = (Boolean)Record.getValue(pRec, 6);
			boolean isPrimaryKey = ((Integer)Record.getValue(pRec, 1)) == RuleMeta.Rule.PrimaryKey.ordinal();
			boolean isUnique = false;
			if (isPrimaryKey) {
				isUnique = true;
			}
			else if (isIndexUnique != null) {
				isUnique = (boolean)isIndexUnique;
			}
			int ruleId = (int)Record.getValue(pRec, 3);
			if (ruleId != this.lastRuleId) {
				this.sequence = 1;
				this.lastRuleId = ruleId;
			}
			item.Table = (String)Record.getValue(pRec, 0);
			item.Non_unique = !isUnique ? 1 : 0;
			item.Key_name = isPrimaryKey ? "PRIMARY" : (String)Record.getValue(pRec, 2);
			item.Seq_in_index = sequence;
			item.Column_name = (String)Record.getValue(pRec, 4);
			item.Collation = "A";
			item.Cardinality = 0;
			item.Null = ((Boolean)Record.getValue(pRec, 5)) ? "YES" : "";
			item.Index_type = "BTREE";
			item.Comment = "";
			item.Index_comment = "";
			try {
				long pResult = newRecord();
				CursorUtil.toRecord(newHeap(), this.meta, pResult, item);
				return pResult;
			}
			catch (Exception x) {
				throw new OrcaException(x);
			}
		}

		@Override
		public void close() {
			super.close();
			this.upstream.close();
		}

	}
	
	public ShowIndex(ObjectName tableName, GeneratorContext ctx) {
		this.tableName = tableName;
		String sql = "select t.table_name, r.rule_type, r.rule_name, r.id, c.column_name, c.nullable, r.is_unique" 
                + " from __sys.systable t"
                + " join __sys.sysrule r on (t.id = r.table_id)"
                + " join __sys.sysrulecol rc on (r.id = rc.rule_id)"
                + " join __sys.syscolumn c on (rc.column_id = c.id)"
                + " where t.namespace=? and t.table_name=?";
		this.script = ctx.getSession().parse(sql);
	}

	@Override
	public Object run(VdmContext ctx, Parameters params, long pMaster) {
		TableMeta table = Checks.tableExist(ctx.getSession(), this.tableName);
		Cursor c = (Cursor)script.run(
				ctx, 
				new Parameters(new Object[]{table.getNamespace(), table.getTableName()}), 
				pMaster);
		c = new Transformer(c);
		return c;
	}

	@Override
	public CursorMeta getCursorMeta() {
		return ShowIndex.META;
	}

	@Override
	public void explain(int level, List<ExplainRecord> records) {
		this.script.root.explain(level, records);
	}
	
}
