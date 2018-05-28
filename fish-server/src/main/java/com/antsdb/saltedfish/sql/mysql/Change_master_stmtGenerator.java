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
package com.antsdb.saltedfish.sql.mysql;

import com.antsdb.saltedfish.lexer.MysqlParser.Change_master_stmtContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Change_master_stmt_optionContext;
import com.antsdb.saltedfish.sql.Generator;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.vdm.ChangeMaster;
import com.antsdb.saltedfish.sql.vdm.Instruction;

/**
 * 
 * @author wgu0
 */
public class Change_master_stmtGenerator extends Generator<Change_master_stmtContext> {

	@Override
	public Instruction gen(GeneratorContext ctx, Change_master_stmtContext rule) throws OrcaException {
		ChangeMaster step = new ChangeMaster();
		for (Change_master_stmt_optionContext i:rule.change_master_stmt_option()) {
			String option = i.WORD().getText();
			if (option.equalsIgnoreCase("MASTER_LOG_FILE")) {
				String s = i.literal_value().getText();
				s = s.substring(1, s.length()-1);
				step.setLogFile(s);
			}
			else if (option.equalsIgnoreCase("MASTER_LOG_POS")) {
				step.setLogPos(Long.parseLong(i.literal_value().getText()));
			}
			else {
				throw new OrcaException("invalid option: " + option);
			}
		}
		return step;
	}

}
