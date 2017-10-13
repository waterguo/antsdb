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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;

import com.antsdb.saltedfish.lexer.MysqlParser.*;
import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.sql.Generator;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.sql.vdm.Checks;
import com.antsdb.saltedfish.sql.vdm.InsertSingleRow;
import com.antsdb.saltedfish.sql.vdm.Instruction;
import com.antsdb.saltedfish.sql.vdm.ObjectName;
import com.antsdb.saltedfish.sql.vdm.OpIncrementColumnValue;
import com.antsdb.saltedfish.sql.vdm.Operator;
import com.antsdb.saltedfish.util.CodingError;

public class Insert_stmtGenerator extends Generator<Insert_stmtContext> {

    @Override
    public Instruction gen(GeneratorContext ctx, Insert_stmtContext rule)
    throws OrcaException {
        ObjectName name = TableName.parse(ctx, rule.table_name_());
        if (rule.insert_stmt_values() != null) {
            boolean isReplace = rule.K_REPLACE() != null;
        	Instruction step = gen(ctx, name, rule.insert_stmt_values(), isReplace, rule.K_IGNORE() != null);
            return step;
        }
        else if (rule.insert_stmt_select() != null) {
            throw new NotImplementedException();
        }
        else {
            throw new CodingError();
        }
    }

    private Instruction gen(
    		GeneratorContext ctx, 
    		ObjectName tableName, 
    		Insert_stmt_valuesContext rule, 
    		boolean isReplace,
    		boolean ignoreError) {
        // collect column names
        
        TableMeta table = Checks.tableExist(ctx.getSession(), tableName);
        tableName = table.getObjectName();
        List<ColumnMeta> fields = new ArrayList<ColumnMeta>();
        if (rule.insert_stmt_values_columns() != null) {
            for (Column_nameContext i :rule.insert_stmt_values_columns().column_name()) {
            	String columnName = Utils.getIdentifier(i.identifier());
            	ColumnMeta iii = Checks.columnExist(table, columnName);	
            	fields.add(iii);
            }
        }
        else {
        	fields.addAll(table.getColumns());
        }
        
        // auto increment column

        List<Operator> defaultValues = new ArrayList<>();
        ColumnMeta autoIncrement = table.findAutoIncrementColumn();
    	int posAutoIncrement = -1;
        if (autoIncrement != null) {
        	posAutoIncrement = fields.indexOf(autoIncrement);
        	if (posAutoIncrement < 0) {
	        	fields.add(autoIncrement);
	        	defaultValues.add(new OpIncrementColumnValue(table, null));
        	}
        }
        
        // columns with default value
        
        for (ColumnMeta i:table.getColumns()) {
        	if (i.getDefault() == null) {
        		continue;
        	}
        	if (fields.contains(i)) {
        		continue;
        	}
        	Operator expr = ExprGenerator.gen(ctx, null, i.getDefault());
        	fields.add(i);
        	defaultValues.add(expr);
        }
        
        // collect expressions
        
        List<List<Operator>> rows = new ArrayList<>();
        for (Insert_stmt_values_rowContext i: rule.insert_stmt_values_row()) {
            List<Operator> values = new ArrayList<>();
            for (ExprContext j:i.expr()) {
            	Operator jj = ExprGenerator.gen(ctx, null, j);
            	if (values.size() == posAutoIncrement) {
            		jj = new OpIncrementColumnValue(table, jj);
            	}
            	values.add(jj);
            }
            values.addAll(defaultValues);
            if (fields.size() != values.size()) {
                throw new OrcaException("number of columns is not matching number of values");
            }

            // auto casting according to column data type
            
            Utils.applyCasting(fields, values);
            
            // end
            
            rows.add(values);
        }
        
        // all set
        
        GTable gtable = ctx.getGtable(tableName);
        InsertSingleRow insert = new InsertSingleRow(ctx.getOrca(), table, gtable, fields, rows);
        insert.setReplace(isReplace);
        insert.setIgnoreError(ignoreError);
        return insert;
    }

}
