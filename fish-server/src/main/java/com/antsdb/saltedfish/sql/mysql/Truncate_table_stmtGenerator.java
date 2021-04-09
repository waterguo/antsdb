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

import com.antsdb.saltedfish.lexer.MysqlParser.*;
import com.antsdb.saltedfish.sql.DdlGenerator;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.vdm.Instruction;
import com.antsdb.saltedfish.sql.vdm.ObjectName;
import com.antsdb.saltedfish.sql.vdm.TruncateTable;

public class Truncate_table_stmtGenerator extends DdlGenerator<Truncate_table_stmtContext>{

    @Override
    public boolean isTemporaryTable(GeneratorContext ctx, Truncate_table_stmtContext rule) {
        ObjectName name = TableName.parse(ctx, rule.table_name_());
        return isTemporaryTable(ctx, name);
    }

    @Override
    public Instruction gen(GeneratorContext ctx, Truncate_table_stmtContext rule) throws OrcaException {
        ObjectName name = TableName.parse(ctx, rule.table_name_());
        return new TruncateTable(name);
    }
}
