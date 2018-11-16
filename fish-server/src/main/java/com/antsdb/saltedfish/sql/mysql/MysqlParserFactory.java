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

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;

import com.antsdb.saltedfish.lexer.MysqlLexer;
import com.antsdb.saltedfish.lexer.MysqlParser;
import com.antsdb.saltedfish.lexer.MysqlParser.Alter_table_stmtContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Create_database_stmtContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Create_table_stmtContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Drop_database_stmtContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Drop_index_stmtContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Drop_table_stmtContext;
import com.antsdb.saltedfish.lexer.MysqlParser.ScriptContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Sql_stmtContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Truncate_table_stmtContext;
import com.antsdb.saltedfish.sql.DdlGenerator;
import com.antsdb.saltedfish.sql.Generator;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.Session;
import com.antsdb.saltedfish.sql.SqlParserFactory;
import com.antsdb.saltedfish.sql.mysql.InstructionGenerator;
import com.antsdb.saltedfish.sql.vdm.Instruction;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.Script;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.util.UberUtil;

public class MysqlParserFactory extends SqlParserFactory {
    static Logger _log = UberUtil.getThisLogger();
    
    @Override
    public Script parse(Session session, CharStream cs) {
        ScriptContext scriptCtx = parse(cs);
        GeneratorContext ctx = new GeneratorContext(session);
        InstructionGenerator.scan(ctx, scriptCtx);
        Instruction code = (Instruction)InstructionGenerator.generate(ctx, scriptCtx);
        Script script = new Script(code, ctx.getParameterCount(), ctx.getVariableCount(), cs.toString());
        return script;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, CharStream cs) {
        ScriptContext rule = parse(cs);
        GeneratorContext gctx = new GeneratorContext(ctx.getSession());
        gctx.setCompileDdl(true);
        InstructionGenerator.scan(gctx, rule);
        Object result = run(gctx, ctx, params, rule);
        return result;
    }

    private Object run(GeneratorContext gctx, VdmContext ctx, Parameters params, ScriptContext rule) {
        Object result = null;
        for (Sql_stmtContext i:rule.sql_stmt()) {
            result = run(gctx, ctx, params, i);
        }
        return result;
    }

    private Object run(GeneratorContext gctx, VdmContext ctx, Parameters params, Sql_stmtContext rule) {
        ParseTree stmt = rule.getChild(0);
        Generator<ParseTree> generator = InstructionGenerator.getGenerator(stmt);
        boolean isDdl = generator instanceof DdlGenerator<?>;
        Instruction code = generator.gen(gctx, stmt);
        boolean success = false;
        if (code == null) {
            return null;
        }
        try {
            Object result = code.run(ctx, params, 0);
            logDdl(ctx, rule);
            success = true;
            return result;
        }
        finally {
            if (isDdl) {
                if (success) {
                    ctx.getSession().commit();
                }
                else {
                    ctx.getSession().rollback();
                }
            }
        }
    }

    static MysqlParser.ScriptContext parse(String sql) {
        return parse(new ANTLRInputStream(sql));
    }
    
    static MysqlParser.ScriptContext parse(CharStream cs) {
        MysqlLexer lexer = new MysqlLexer(cs);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        tokens.setTokenSource(lexer);
        MysqlParser parser = new MysqlParser(tokens);
        parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
        parser.setErrorHandler(new BailErrorStrategy());
        boolean success = false;
        try {
            MysqlParser.ScriptContext script = parser.script();
            success = true;
            return script;
        }
        finally {
            if (!success && (parser.lastStatement != null)) {
                _log.debug("last passed statement: {}", ((ParseTree)parser.lastStatement).getText());
            }
        }
    }

    private void logDdl(VdmContext ctx, Sql_stmtContext rule) {
        if (isLoggableDdl(rule.getChild(0))) {
            String sql = toSql(rule.getChild(0));
            String namespace = ctx.getSession().getCurrentNamespace();
            if (StringUtils.isEmpty(namespace)) {
                sql = "!!" + sql;
            }
            else {
                sql = "!!use " + ctx.getSession().getCurrentNamespace() + " ; " + sql;
            }
            ctx.getHumpback().getGobbler().logMessage(ctx.getHSession(), sql);
        }
    }

    private boolean isLoggableDdl(ParseTree tree) {
        if (tree instanceof Create_table_stmtContext) {
            return true;
        }
        else if (tree instanceof Create_table_like_stmtGenerator) {
            return true;
        }
        else if (tree instanceof Create_database_stmtContext) {
            return true;
        }
        else if (tree instanceof Create_index_stmtGenerator) {
            return true;
        }
        else if (tree instanceof Alter_table_stmtContext) {
            return true;
        }
        else if (tree instanceof Drop_database_stmtContext) {
            return true;
        }
        else if (tree instanceof Drop_index_stmtContext) {
            return true;
        }
        else if (tree instanceof Drop_table_stmtContext) {
            return true;
        }
        else if (tree instanceof Truncate_table_stmtContext) {
            return true;
        }
        else {
            return false;
        }
    }
    
    private String toSql(ParseTree tree) {
        StringBuilder buf = new StringBuilder();
        toText(buf, tree);
        return buf.toString();
    }

    private void toText(StringBuilder buf, ParseTree tree) {
        for (int i=0; i<tree.getChildCount(); i++) {
            ParseTree ii = tree.getChild(i);
            if (ii instanceof TerminalNode) {
                buf.append(ii.getText());
                buf.append(' ');
            }
            else {
                toText(buf, ii);
            }
        }
    }
}
