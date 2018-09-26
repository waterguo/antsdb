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

import java.util.ArrayList;
import java.util.List;

import com.antsdb.saltedfish.lexer.FishParser.Hupdate_stmtContext;
import com.antsdb.saltedfish.lexer.FishParser.Update_setContext;
import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.sql.Generator;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.vdm.CursorMaker;
import com.antsdb.saltedfish.sql.vdm.HUpdate;
import com.antsdb.saltedfish.sql.vdm.Instruction;
import com.antsdb.saltedfish.sql.vdm.Operator;

/**
 * 
 * @author *-xguo0<@
 */
public class Hupdate_stmtGenerator extends Generator<Hupdate_stmtContext> {

    @Override
    public Instruction gen(GeneratorContext ctx, Hupdate_stmtContext rule) throws OrcaException {
        int tableId = Integer.parseInt(rule.table().getText());
        CursorMaker maker = HCrudUtil.gen(ctx, tableId, rule.where());
        GTable gtable = HCrudUtil.getTable(ctx, tableId);
        List<Integer> columns = new ArrayList<>();
        List<Operator> values = new ArrayList<>();
        for (Update_setContext i:rule.update_sets().update_set()) {
            Integer columndId = Integer.parseInt(HCrudUtil.genColumn(maker, i.column()).getName());
            Operator value = HCrudUtil.genValue(i.value());
            columns.add(columndId);
            values.add(value);
        }
        return new HUpdate(gtable, maker, columns, values);
    }

}
