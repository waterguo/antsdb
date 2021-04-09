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

import java.util.List;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.misc.Interval;

import com.antsdb.saltedfish.lexer.MysqlParser.Create_view_stmtContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Select_stmtContext;
import com.antsdb.saltedfish.sql.Generator;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.vdm.CreateColumn;
import com.antsdb.saltedfish.sql.vdm.CreateView;
import com.antsdb.saltedfish.sql.vdm.CursorMaker;
import com.antsdb.saltedfish.sql.vdm.DropTable;
import com.antsdb.saltedfish.sql.vdm.FieldMeta;
import com.antsdb.saltedfish.sql.vdm.Flow;
import com.antsdb.saltedfish.sql.vdm.Instruction;
import com.antsdb.saltedfish.sql.vdm.ObjectName;

/**
 * 
 * @author *-xguo0<@
 */
public class Create_view_stmtGenerator extends Generator<Create_view_stmtContext>{

    @Override
    public Instruction gen(GeneratorContext ctx, Create_view_stmtContext rule) throws OrcaException {
        String sql = getText(rule.select_stmt());
        ObjectName name = TableName.parse(ctx, rule.table_name_());
        Flow result = new Flow();
        if (rule.K_REPLACE() != null) {
            result.add(new DropTable(name, true));
        }
        result.add(new CreateView(name, sql));
        CursorMaker maker = Select_stmtGenerator.gen(ctx, rule.select_stmt(), null).run();
        List<String> columns = Utils.getColumns(rule.columns());
        if (columns != null) {
            if (columns.size() != maker.getCursorMeta().getColumnCount()) {
                throw new OrcaException("number of fields from the SELECT does not match the number of column list");
            }
        }
        for (int i=0; i<maker.getCursorMeta().getColumnCount(); i++) {
            FieldMeta ii = maker.getCursorMeta().getColumn(i);
            CreateColumn cc = new CreateColumn();
            cc.type = ii.getType();
            cc.tableName = name;
            cc.columnName = (columns != null) ? columns.get(i) : ii.getName();
            result.add(cc);
        }
        return result;
    }

    private String getText(Select_stmtContext rule) {
        CharStream input = rule.start.getInputStream();
        return input.getText(new Interval(rule.start.getStartIndex(), rule.stop.getStopIndex()));
    }

}
