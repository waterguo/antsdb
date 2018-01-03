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

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;

import com.antsdb.saltedfish.lexer.MysqlParser.Lock_table_itemContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Lock_table_stmtContext;
import com.antsdb.saltedfish.sql.Generator;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.vdm.Instruction;
import com.antsdb.saltedfish.sql.vdm.LockTable;
import com.antsdb.saltedfish.sql.vdm.ObjectName;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author wgu0
 */
public class Lock_table_stmtGenerator extends Generator<Lock_table_stmtContext> {
	static Logger _log = UberUtil.getThisLogger();
	
	@Override
	public Instruction gen(GeneratorContext ctx, Lock_table_stmtContext rule) throws OrcaException {
		List<ObjectName> names = new ArrayList<>();
		for (Lock_table_itemContext i:rule.lock_table_item()) {
			ObjectName name = TableName.parse(ctx, i.table_name_());
			names.add(name);
		}
		return new LockTable(names);
	}

}
