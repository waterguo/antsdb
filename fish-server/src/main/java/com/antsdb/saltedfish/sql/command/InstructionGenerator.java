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
package com.antsdb.saltedfish.sql.command;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.commons.lang.StringUtils;

import com.antsdb.saltedfish.sql.CompileDdlException;
import com.antsdb.saltedfish.sql.DdlGenerator;
import com.antsdb.saltedfish.sql.Generator;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.vdm.Instruction;
import com.antsdb.saltedfish.sql.vdm.Statement;
import com.antsdb.saltedfish.sql.vdm.StatementWrapper;

/**
 * 
 * @author *-xguo0<@
 */
class InstructionGenerator {
    static Map<Class<?>, Generator<ParseTree>> _generatorByName = new ConcurrentHashMap<>();

    static public Instruction generate(GeneratorContext ctx, ParseTree rule) throws OrcaException {
        Generator<ParseTree> generator = getGenerator(rule);
        Instruction code = generator.gen(ctx, rule);
        if (!ctx.isCompileDdl() && (generator instanceof DdlGenerator<?>)) {
            // run it if it is ddl statement otherwise following statements may fail to parse because of missing
            // objects
            CompileDdlException x = new CompileDdlException();
            x.nParameters = ctx.getParameterCount();
            throw x;
        }
        else if (code instanceof Statement) {
            Statement stmt = (Statement)code;
            StatementWrapper wrapper = new StatementWrapper(stmt, rule, generator);
            return wrapper;
        }
        else {
            return code;
        }
    }
    
    @SuppressWarnings("unchecked")
    static public Generator<ParseTree> getGenerator(ParseTree ctx) throws OrcaException {
        Class<?> klass = ctx.getClass();
        Generator<ParseTree> generator = _generatorByName.get(klass);
        if (generator == null) {
            String key = StringUtils.removeStart(klass.getSimpleName(), "FishParser$");
            key = StringUtils.removeEnd(key, "Context");
            key += "Generator";
            try {
                key = InstructionGenerator.class.getPackage().getName() + "." + key;
                Class<?> generatorClass = Class.forName(key);
                generator = (Generator<ParseTree>)generatorClass.newInstance();
                _generatorByName.put(klass, generator);
            }
            catch (Exception x) {
                throw new OrcaException("instruction geneartor is not found: " + key, x);
            }
        }
        return generator;
    }
}
