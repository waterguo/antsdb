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

import java.util.Map;

import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.MetadataService;
import com.antsdb.saltedfish.sql.meta.SequenceMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.sql.vdm.Checks;
import com.antsdb.saltedfish.sql.vdm.ObjectName;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.Statement;
import com.antsdb.saltedfish.sql.vdm.Transaction;
import com.antsdb.saltedfish.sql.vdm.VdmContext;

public class SyncTableSequence extends Statement {
	ObjectName tableName;
	Map<String, String> tableOptions;
	
	public SyncTableSequence(ObjectName tableName, Map<String, String> options) {
		this.tableName = tableName;
		this.tableOptions = options;
	}

	@Override
	public Object run(VdmContext ctx, Parameters params) {
		int autoIncrement = 1;
		if (this.tableOptions != null) {
			if (this.tableOptions.containsKey("AUTO_INCREMENT")) {
				autoIncrement = Integer.parseInt(this.tableOptions.get("AUTO_INCREMENT"));
			}
		}
        MetadataService meta = ctx.getOrca().getMetaService();
        Transaction trx = ctx.getTransaction();
		TableMeta table = Checks.tableExist(ctx.getSession(), this.tableName);
		if (table.getId() < 0) {
			// don't do anything to system tables. at the initialization stage, it will crash in the following
			// steps
			return null;
		}
		ColumnMeta column = table.findAutoIncrementColumn();
		ObjectName name = table.getAutoIncrementSequenceName();
		SequenceMeta seq = meta.getSequence(trx, name);
		if ((column != null) && (seq == null)) {
	        seq = new SequenceMeta(ctx.getOrca(), name);
	        seq.setLastNumber(autoIncrement-1);
	        seq.setSeed(1);
	        seq.setIncrement(1);
	        meta.addSequence(trx, seq);
		}
		else if ((column == null) && (seq != null)) {
			meta.dropSequence(trx, seq);
		}
		return null;
	}

}
