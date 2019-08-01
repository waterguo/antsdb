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

import com.antsdb.saltedfish.lexer.FishParser.Compare_cache_stmtContext;
import com.antsdb.saltedfish.sql.Generator;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.vdm.CompareCache;
import com.antsdb.saltedfish.sql.vdm.Instruction;

/**
 * 
 * @author *-xguo0<@
 */
public class Compare_cache_stmtGenerator extends Generator<Compare_cache_stmtContext>{

    @Override
    public Instruction gen(GeneratorContext ctx, Compare_cache_stmtContext rule) throws OrcaException {
        CompareCache result;
        if (rule.number_value() != null) {
            int tableId = Integer.parseInt(rule.number_value().getText());
            result = new CompareCache(tableId);
        }
        else {
            result = new CompareCache();
        }
        result.setOptions(Utils.getProperties(rule.assignment()));
        return result;
    }

}
