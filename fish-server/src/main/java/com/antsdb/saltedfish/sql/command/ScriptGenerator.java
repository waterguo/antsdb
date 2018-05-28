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
package com.antsdb.saltedfish.sql.command;

import java.util.List;

import org.antlr.v4.runtime.tree.ParseTree;

import com.antsdb.saltedfish.lexer.FishParser.ScriptContext;
import com.antsdb.saltedfish.lexer.FishParser.StmtContext;
import com.antsdb.saltedfish.sql.Generator;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.vdm.Flow;
import com.antsdb.saltedfish.sql.vdm.Instruction;

/**
 * 
 * @author *-xguo0<@
 */
class ScriptGenerator extends Generator<ScriptContext>{

    @Override
    public Instruction gen(GeneratorContext ctx, ScriptContext rule) throws OrcaException {
        List<StmtContext> stmts = rule.stmt();
        if (stmts.size() == 0) {
            return new Flow();
        }
        else if (stmts.size() == 1) {
            return InstructionGenerator.generate(ctx, stmts.get(0));
        }
        else {
            Flow flow = new Flow();
            for (ParseTree i:stmts) {
                Instruction step = InstructionGenerator.generate(ctx, i);
                if (step != null) {
                    flow.add(step);
                }
            }
            return flow;
        }
    }
}
