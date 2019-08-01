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

import com.antsdb.saltedfish.lexer.MysqlParser.Explain_stmtContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Select_or_valuesContext;
import com.antsdb.saltedfish.sql.Generator;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.planner.Planner;
import com.antsdb.saltedfish.sql.vdm.Analyze;
import com.antsdb.saltedfish.sql.vdm.Explain;
import com.antsdb.saltedfish.sql.vdm.Instruction;
import com.antsdb.saltedfish.sql.vdm.Profile;

public class Explain_stmtGenerator extends Generator<Explain_stmtContext> {

    @Override
    public Instruction gen(GeneratorContext ctx, Explain_stmtContext rule) throws OrcaException {
        if (rule.K_ANALYZE() != null) {
            Select_or_valuesContext select = rule.sql_stmt().select_stmt().select_or_values(0);
            Planner planner = Select_or_valuesGenerator.gen(ctx, select, null);
            planner.analyze();
            return new Analyze(planner);
        }
        Instruction step = InstructionGenerator.generate(ctx, rule.sql_stmt());
        if (rule.K_PROFILE() != null) {
            return new Profile(step);
        }
        else {
            return new Explain(step);
        }
    }

}
