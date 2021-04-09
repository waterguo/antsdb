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

import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import com.antsdb.saltedfish.lexer.FishLexer;
import com.antsdb.saltedfish.lexer.FishParser;
import com.antsdb.saltedfish.lexer.FishParser.ScriptContext;
import com.antsdb.saltedfish.lexer.FishParser.StmtContext;
import com.antsdb.saltedfish.sql.DdlGenerator;
import com.antsdb.saltedfish.sql.Generator;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.Session;
import com.antsdb.saltedfish.sql.SqlParserFactory;
import com.antsdb.saltedfish.sql.vdm.Instruction;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.Script;
import com.antsdb.saltedfish.sql.vdm.VdmContext;

/**
 * 
 * @author *-xguo0<@
 */
public class FishParserFactory extends SqlParserFactory {

    @Override
    public Script parse(GeneratorContext ctx, Session session, CharStream cs) {
        ScriptContext scriptCtx = parse(cs);
        if (ctx == null) ctx = new GeneratorContext(session);
        Instruction code = (Instruction)InstructionGenerator.generate(ctx, scriptCtx);
        Script script = new Script(code, ctx.getParameterCount(), ctx.getVariableCount(), cs.toString());
        return script;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, CharStream cs) {
        ScriptContext rule = parse(cs);
        GeneratorContext gctx = new GeneratorContext(ctx.getSession());
        Object result = run(gctx, ctx, params, rule);
        return result;
    }

    private Object run(GeneratorContext gctx, VdmContext ctx, Parameters params, ScriptContext rule) {
        Object result = null;
        for (StmtContext i:rule.stmt()) {
            result = run(gctx, ctx, params, i);
        }
        return result;
    }

    private Object run(GeneratorContext gctx, VdmContext ctx, Parameters params, StmtContext rule) {
        ParseTree stmt = rule.getChild(0);
        Generator<ParseTree> generator = InstructionGenerator.getGenerator(stmt);
        Instruction code = generator.gen(gctx, stmt);
        if (code == null) {
            return null;
        }
        try {
            Object result = code.run(ctx, params, 0);
            return result;
        }
        finally {
            if (generator instanceof DdlGenerator<?>) {
                // mysql commits after every ddl statement
                ctx.getSession().commit();
            }
        }
    }

    static FishParser.ScriptContext parse(CharStream cs) {
        FishLexer lexer = new FishLexer(cs);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        tokens.setTokenSource(lexer);
        FishParser parser = new FishParser(tokens);
        parser.setErrorHandler(new BailErrorStrategy());
        FishParser.ScriptContext script = parser.script();
        return script;
    }
}
