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

import java.io.File;

import org.apache.commons.lang.StringUtils;

import com.antsdb.saltedfish.lexer.MysqlParser.Load_data_infile_stmtContext;
import com.antsdb.saltedfish.sql.Generator;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.vdm.Instruction;
import com.antsdb.saltedfish.sql.vdm.LoadCsv;
import com.antsdb.saltedfish.sql.vdm.ObjectName;

public class Load_data_infile_stmtGenerator extends Generator<Load_data_infile_stmtContext> {

    @Override
    public Instruction gen(GeneratorContext ctx, Load_data_infile_stmtContext rule)
    throws OrcaException {
        String filename = rule.file_name().getText();
        filename = StringUtils.removeStart(filename, "\'");
        filename = StringUtils.removeEnd(filename, "\'");
        ObjectName tableName = TableName.parse(ctx, rule.table_name_());
        LoadCsv instru = new LoadCsv(new File(filename), tableName);
        return instru;
    }

}
