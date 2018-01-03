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

import org.apache.commons.lang.NotImplementedException;

import com.antsdb.saltedfish.lexer.MysqlParser.Names_valueContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Set_stmtContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Set_stmt_character_setContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Set_stmt_namesContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Set_stmt_variableContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Variable_assignmentContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Variable_assignment_globalContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Variable_assignment_global_transactionContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Variable_assignment_sessionContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Variable_assignment_session_transactionContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Variable_assignment_transactionContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Variable_assignment_userContext;
import com.antsdb.saltedfish.sql.Generator;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.vdm.Flow;
import com.antsdb.saltedfish.sql.vdm.Instruction;
import com.antsdb.saltedfish.sql.vdm.Operator;
import com.antsdb.saltedfish.sql.vdm.SetSystemParameter;
import com.antsdb.saltedfish.sql.vdm.SetSystemParameter.Scope;
import com.antsdb.saltedfish.sql.vdm.SetVariable;
import com.antsdb.saltedfish.util.CodingError;

public class Set_stmtGenerator extends Generator<Set_stmtContext> {

    @Override
    public Instruction gen(GeneratorContext ctx, Set_stmtContext rule)
    throws OrcaException {
        if (rule.set_stmt_names() != null) {
            return genSetNames(ctx, rule.set_stmt_names());
        }
        if (rule.set_stmt_character_set() != null) {
            return genSetCharSet(ctx, rule.set_stmt_character_set());
        }
        else if (rule.set_stmt_variable() != null){
            return genSetVariables(ctx, rule.set_stmt_variable());
        }
        else {
            throw new NotImplementedException();
        }
    }

    private Instruction genSetVariables(GeneratorContext ctx, Set_stmt_variableContext rule) {
        Flow flow = new Flow();
        for (Variable_assignmentContext i:rule.variable_assignment()) {
            Instruction set = null;
            if (i.variable_assignment_user() != null) {
                set = createSetUserVariable(ctx, i.variable_assignment_user());
            }
            else if (i.variable_assignment_session() != null) {
                set = createSetSessionVariable(ctx, i.variable_assignment_session());
            }
            else if (i.variable_assignment_global() != null) {
                SetSystemParameter ssp = createSetGlobalVariable(ctx, i.variable_assignment_global());
                ssp.setPermanent(rule.K_PERMANENT() != null);
                set = ssp;
            }
            else if (i.variable_assignment_session_transaction() != null) {
                set = createSetSessionTransaction(ctx, i.variable_assignment_session_transaction());
            }
            else if (i.variable_assignment_global_transaction() != null) {
                SetSystemParameter ssp = createSetGlobalTransaction(ctx, i.variable_assignment_global_transaction());
                ssp.setPermanent(rule.K_PERMANENT() != null);
                set = ssp;
            }
            else if (i.variable_assignment_transaction() != null) {
                set = createSetTransaction(ctx, i.variable_assignment_transaction());
            }
            else {
                throw new Error();
            }
            flow.add(set);
        }
        return flow;
    }

    private Instruction genSetNames(GeneratorContext ctx, Set_stmt_namesContext rule) {
        String name = rule.names_value().getText();
		if (rule.names_value().STRING_LITERAL() != null) {
			name = name.substring(1, name.length()-1);
		}
		else if (rule.names_value().DOUBLE_QUOTED_LITERAL() != null) {
			name = name.substring(1, name.length()-1);
		}
		
		Flow flow = new Flow();
	    flow.add(new SetSystemParameter(Scope.SESSION, "character_set_client", name));
        flow.add(new SetSystemParameter(Scope.SESSION, "character_set_results", name));
        flow.add(new SetSystemParameter(Scope.SESSION, "character_set_connection", name));
        return flow;
    }

    private Instruction genSetCharSet(GeneratorContext ctx, Set_stmt_character_setContext rule) {
        // test on 5.5.57-MariaDB shows SET CHARACTER SET only affects character_set_results. 
        String name = rule.names_value().getText();
        if (rule.names_value().STRING_LITERAL() != null) {
            name = name.substring(1, name.length()-1);
        }
        Flow flow = new Flow();
        flow.add(new SetSystemParameter(Scope.SESSION, "character_set_results", name));
        return flow;
    }

    private SetSystemParameter createSetGlobalVariable(GeneratorContext ctx,Variable_assignment_globalContext rule) 
    throws OrcaException {
        String name = rule.any_name().getText();
        SetSystemParameter set;

        // set to default value
        
        if (rule.set_expr().K_DEFAULT() != null) {
            set = new SetSystemParameter(SetSystemParameter.Scope.GLOBAL, name);
        }
        
        // set to constant
        
        else if (rule.set_expr().names_value() != null) {
            Names_valueContext nmctx = rule.set_expr().names_value();
            String nval = genNamesValue(nmctx);
            set = new SetSystemParameter(SetSystemParameter.Scope.GLOBAL, name, nval);
        }
        
        // set to an expression value
        
        else if (rule.set_expr().expr() != null) {
            Operator expr = ExprGenerator.gen(ctx, null, rule.set_expr().expr());
            set = new SetSystemParameter(SetSystemParameter.Scope.GLOBAL, name, expr);
        }
        else {
            throw new CodingError();
        }
        
        return set;
    }

    private SetSystemParameter createSetSessionVariable(GeneratorContext ctx, Variable_assignment_sessionContext rule) 
    throws OrcaException {
        String name = rule.session_variable_name().getText();
        if (name.startsWith("@@")) {
            name = name.substring(2);
        }
        SetSystemParameter set;

        // set to default value
        
        if (rule.set_expr().K_DEFAULT() != null) {
            set = new SetSystemParameter(SetSystemParameter.Scope.SESSION, name);
        }
        
        // set to constant
        
        else if (rule.set_expr().names_value() != null) {
            Names_valueContext nmctx = rule.set_expr().names_value();
            String nval = genNamesValue(nmctx);
            set = new SetSystemParameter(SetSystemParameter.Scope.SESSION, name, nval);
        }
        
        // set to an expression value
        
        else if (rule.set_expr().expr() != null) {
            Operator expr = ExprGenerator.gen(ctx, null, rule.set_expr().expr());
            set = new SetSystemParameter(SetSystemParameter.Scope.SESSION, name, expr);
        }
        else {
            throw new CodingError();
        }
        
        return set;
    }

    private SetVariable createSetUserVariable(GeneratorContext ctx, Variable_assignment_userContext rule) 
    throws OrcaException {
        Operator expr = ExprGenerator.gen(ctx, null, rule.expr());
        String name = rule.user_var_name().getText().substring(1);
        SetVariable set = new SetVariable(name, expr);
        return set;
    }

    private SetSystemParameter createSetGlobalTransaction(GeneratorContext ctx,Variable_assignment_global_transactionContext rule) 
    throws OrcaException {
        	// Transaction setting is not fully supported. 
        	// We won't handle dirty read, etc
        	SetSystemParameter set = new SetSystemParameter(SetSystemParameter.Scope.GLOBAL, "TRANSACTION", "");
        	return set;
    }

    private SetSystemParameter createSetSessionTransaction(GeneratorContext ctx, Variable_assignment_session_transactionContext rule) 
    throws OrcaException {
        	// Transaction setting is not fully supported. 
        	// We won't handle dirty read, etc
        	SetSystemParameter set = new SetSystemParameter(SetSystemParameter.Scope.SESSION, "TRANSACTION", "");
        	return set;
    }

    private SetSystemParameter createSetTransaction(GeneratorContext ctx, Variable_assignment_transactionContext rule) 
    throws OrcaException {
        	// Transaction setting is not fully supported. 
        	// We won't handle dirty read, etc
        	SetSystemParameter set = new SetSystemParameter(SetSystemParameter.Scope.SESSION, "TRANSACTION", "");
        	return set;
    }
    
    private String genNamesValue(Names_valueContext nmctx)
    {
        String nval = nmctx.getText();
		if (nmctx.STRING_LITERAL() != null) {
			nval = nval.substring(1, nval.length()-1);
		}
		else if (nmctx.DOUBLE_QUOTED_LITERAL() != null) {
			nval = nval.substring(1, nval.length()-1);
		}
		return nval;
    }
}
