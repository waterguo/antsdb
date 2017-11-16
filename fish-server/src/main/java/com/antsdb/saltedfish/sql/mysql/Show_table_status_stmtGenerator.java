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

import com.antsdb.saltedfish.lexer.MysqlParser.Show_table_status_stmtContext;
import com.antsdb.saltedfish.sql.Generator;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.vdm.Instruction;

/**
 * 
 * @author *-xguo0<@
 */
public class Show_table_status_stmtGenerator extends Generator<Show_table_status_stmtContext> {
    @Override
    public Instruction gen(GeneratorContext ctx, Show_table_status_stmtContext rule) throws OrcaException {
        String db = null;
        String like = null;
        if (rule.K_FROM() != null) {
            db = rule.identifier().getText();
        }
        if (rule.K_LIKE() != null) {
            like = Utils.getLiteralValue(rule.string_value());
        }
        ShowTableStatus stmt = new ShowTableStatus(db, like);
        return stmt;
    }
}
