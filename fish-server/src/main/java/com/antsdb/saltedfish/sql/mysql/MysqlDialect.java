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

import com.antsdb.saltedfish.sql.DataTypeFactory;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.SqlDialect;
import com.antsdb.saltedfish.sql.SqlParserFactory;

public class MysqlDialect extends SqlDialect {
	static {
		Orca.registerDialect(new MysqlDialect());
	}
	
	@Override
    public void init(Orca orca) {
    	orca.registerSystemView("information_schema", "SCHEMATA", new SCHEMATA(orca));
    	orca.registerSystemView(Orca.SYSNS, "mysql_slave", new MysqlSlaveView());
    }

	@Override
	public SqlParserFactory getParserFactory() {
		return new MysqlParserFactory();
	}

	@Override
	public DataTypeFactory getTypeFactory() {
		return new MysqlDataTypeFactory();
	}

	@Override
	public String getName() {
		return "MYSQL";
	}
}
