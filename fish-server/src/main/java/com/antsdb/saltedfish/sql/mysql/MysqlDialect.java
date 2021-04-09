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
        orca.registerSystemView("mysql", "user", new USER(orca));
        orca.registerSystemView("mysql", "proc", new PROC());
        orca.registerSystemView("information_schema", "SCHEMATA", new SCHEMATA(orca));
        orca.registerSystemView("information_schema", "TABLES", new TABLES(orca));
        orca.registerSystemView("information_schema", "ROUTINES", new ROUTINES(orca));
        orca.registerSystemView("information_schema", "EVENTS", new EVENTS(orca));
        orca.registerSystemView("information_schema", "VIEWS", new VIEWS(orca));
        orca.registerSystemView("information_schema", "COLUMN_PRIVILEGES", new COLUMN_PRIVILEGES(orca));
        orca.registerSystemView("information_schema", "USER_PRIVILEGES", new USER_PRIVILEGES(orca));
        orca.registerSystemView("information_schema", "CHARACTER_SETS", new CHARACTER_SETS(orca));
        orca.registerSystemView("information_schema", "COLLATIONS", new COLLATIONS(orca));
        orca.registerSystemView("information_schema", "TABLE_PRIVILEGES", new TABLE_PRIVILEGES(orca));
        orca.registerSystemView("information_schema", "SCHEMA_PRIVILEGES", new SCHEMA_PRIVILEGES(orca));
        orca.registerSystemView("information_schema", "TRIGGERS", new TRIGGERS(orca));
        orca.registerSystemView("information_schema", "COLUMNS", new COLUMNS(orca));
        orca.registerSystemView("information_schema", "STATISTICS", new STATISTICS());
        orca.registerSystemView("information_schema", "KEY_COLUMN_USAGE", new KEY_COLUMN_USAGE());
        orca.registerSystemView("information_schema", "PARTITIONS", new PARTITIONS());
        orca.registerSystemView("information_schema", "FILES", new FILES());
        orca.registerSystemView("information_schema", "PARAMETERS", new PARAMETERS());
        orca.registerSystemView("information_schema", "PROFILING", new PROFILING());
        orca.registerSystemView("information_schema", "ENGINES", new ENGINES());
        orca.registerSystemView(Orca.SYSNS, "mysql_slave", new MysqlSlaveView());
        
        // empty performance schema views
        final String NS_PERF = "performance_schema";
        orca.registerSystemView(NS_PERF, "events_stages_history_long", new PerfStagesHistoryLong());
        orca.registerSystemView(NS_PERF, "events_statements_current", new PerfEventsStatementsCurrent());
        orca.registerSystemView(NS_PERF, "events_waits_history_long", new PerfWaitsHistoryLong());
        orca.registerSystemView(NS_PERF, "threads", new PerfThreads());
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
