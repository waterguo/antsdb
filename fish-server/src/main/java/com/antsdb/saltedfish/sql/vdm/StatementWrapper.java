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
package com.antsdb.saltedfish.sql.vdm;

import java.util.List;

import org.antlr.v4.runtime.tree.ParseTree;

import com.antsdb.saltedfish.sql.Generator;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.LockLevel;
import com.antsdb.saltedfish.sql.meta.TableMeta;

/**
 * StatementWrapper make sure dependent tables are locked
 * 
 * @author xguo
 *
 */
public class StatementWrapper extends Instruction {
    Statement stmt;
    ParseTree tree;
    Generator<ParseTree> generator;
    
    public StatementWrapper(Statement stmt, ParseTree tree, Generator<ParseTree> generator) {
        super();
        this.stmt = stmt;
        this.tree = tree;
        this.generator = generator;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        List<TableMeta> dependents = this.stmt.getDependents();
        return run(dependents, 0, ctx, params, pMaster);
    }

    private Object run(List<TableMeta> dependents, int i, VdmContext ctx, Parameters params, long pMaster) {
        if (i >= dependents.size()) {
            return run(dependents, ctx, params, pMaster);
        }
        else {
            TableMeta table = dependents.get(i);
            if (!ctx.getSession().isImportModeOn()) {
                // in asynchronous batch mode mode, lock is supposed to be locked already. dont lock again. the session
                // locking mechanism doesn't support concurrency
                ctx.getSession().lockTable(table.getId(), LockLevel.SHARED, true);
            }
            return run(dependents, i+1, ctx, params, pMaster);
        }
    }

    private Object run(List<TableMeta> dependents, VdmContext ctx, Parameters params, long pMaster) {
        // has dependents been modified ? if so, regenerate
        
        for (TableMeta i:dependents) {
            if (i.isAged()) {
                GeneratorContext generatorCtx = new GeneratorContext(ctx.getSession());
                this.stmt = (Statement)this.generator.gen(generatorCtx, this.tree);
                break;
            }
        }
        
        // now run it 
        
        Object result = this.stmt.run(ctx, params, pMaster);
        return result;
    }

    @Override
    public void explain(int level, List<ExplainRecord> records) {
        this.stmt.explain(level, records);
    }

}
