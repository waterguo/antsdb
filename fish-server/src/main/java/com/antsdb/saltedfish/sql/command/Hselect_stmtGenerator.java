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

import com.antsdb.saltedfish.lexer.FishParser.ColumnContext;
import com.antsdb.saltedfish.lexer.FishParser.Hselect_stmtContext;
import com.antsdb.saltedfish.sql.Generator;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.vdm.Aggregator;
import com.antsdb.saltedfish.sql.vdm.CursorMaker;
import com.antsdb.saltedfish.sql.vdm.CursorMeta;
import com.antsdb.saltedfish.sql.vdm.FieldMeta;
import com.antsdb.saltedfish.sql.vdm.FieldValue;
import com.antsdb.saltedfish.sql.vdm.Instruction;
import com.antsdb.saltedfish.sql.vdm.Operator;

/**
 * 
 * @author *-xguo0<@
 */
public class Hselect_stmtGenerator extends Generator<Hselect_stmtContext> {

    @Override
    public Instruction gen(GeneratorContext ctx, Hselect_stmtContext rule) throws OrcaException {
        int tableId = Utils.tableExists(ctx.getSession(), rule.table().getText());
        CursorMaker result = HCrudUtil.gen(ctx, tableId, rule.where(), rule.limit());
        if (rule.columns().column().size() > 0) {
            List<Operator> fields = new ArrayList<>();
            CursorMeta meta = new CursorMeta();
            for (ColumnContext i:rule.columns().column()) {
                String label = i.getText().substring(1);
                boolean found = false;
                for (int j=0; j<result.getCursorMeta().getColumnCount(); j++) {
                    FieldMeta jj = result.getCursorMeta().getColumn(j);
                    if (jj.getName().equals(label)) {
                        FieldValue fv = new FieldValue(jj, j);
                        fields.add(fv);
                        meta.addColumn(jj);
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    throw new OrcaException("column {} is not found", i.getText());
                }
            }
            result = new Aggregator(result, meta, fields, 0); 
        }
        return result;
    }
}
