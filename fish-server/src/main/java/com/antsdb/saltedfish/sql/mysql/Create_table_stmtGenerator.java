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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;

import com.antsdb.saltedfish.lexer.MysqlParser.Column_defContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Constraint_defContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Create_defContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Create_table_stmtContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Index_defContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Primary_key_defContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Table_optionContext;
import com.antsdb.saltedfish.sql.DdlGenerator;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.vdm.CreateColumn;
import com.antsdb.saltedfish.sql.vdm.CreateForeignKey;
import com.antsdb.saltedfish.sql.vdm.CreateIndex;
import com.antsdb.saltedfish.sql.vdm.CreatePrimaryKey;
import com.antsdb.saltedfish.sql.vdm.CreateTable;
import com.antsdb.saltedfish.sql.vdm.Flow;
import com.antsdb.saltedfish.sql.vdm.Instruction;
import com.antsdb.saltedfish.sql.vdm.ObjectName;

public class Create_table_stmtGenerator extends DdlGenerator<Create_table_stmtContext>{

    @Override
    public Instruction gen(GeneratorContext ctx, Create_table_stmtContext rule) throws OrcaException {
        Flow flow = new Flow();
        ObjectName tableName = TableName.parse(ctx, rule.table_name_());
        CreateTable createTable = new CreateTable(tableName);
        flow.add(createTable);
        
        // collect table options
        
        Map<String, String> options = new HashMap<>();
        for (Table_optionContext i:rule.table_options().table_option()) {
        	String option = i.table_option_name().getText();
        	String value = i.table_option_value().getText();
        	options.put(option, value);
        }
        
        // count number of unique keys
        
        int nUniqueKeys = 0;
        
        for (Create_defContext i:rule.create_defs().create_def()) {
            if (i.primary_key_def() != null) {
            	nUniqueKeys++;
            }
            else if (i.index_def() != null) {
            	if (i.index_def().K_UNIQUE() != null) {
            		nUniqueKeys++;
            	}
            	
            }
        }
        
        // create columns and constraints
        
        for (Create_defContext i:rule.create_defs().create_def()) {
            if (i.primary_key_def() != null) {
                flow.add(createPrimrayKey(ctx, tableName, i.primary_key_def()));
            }
            else if (i.index_def() != null) {
            	boolean isUnique = i.index_def().K_UNIQUE() != null;
            	if (isUnique && (nUniqueKeys==1)) {
            		// special treatment, if there is only one unique key, make it as primary key
                    flow.add(createPrimrayKey(ctx, tableName, i.index_def()));
            	}
            	else {
                	flow.add(createIndex(ctx, tableName, i.index_def()));
            	}
            }
            else if (i.column_def() != null) {
            	List<String> keyColumns = new ArrayList<>();
            	CreateColumn cc = createColumn(ctx, i.column_def(), tableName, keyColumns);
                flow.add(cc);
                if (keyColumns.size() > 0) {
                	CreatePrimaryKey cpk = new CreatePrimaryKey(tableName, keyColumns);
                	flow.add(cpk);
                }
                else if (keyColumns.size() > 1) {
                	throw new OrcaException("Multiple primary key defined");
                }
            }
            else if (i.constraint_def() != null) {
            	Instruction step = createConstraint(ctx, tableName, i.constraint_def());
            	flow.add(step);
            }
            else {
            	throw new NotImplementedException();
            }
        }
        flow.add(new SyncTableSequence(tableName, options));
        return flow;
    }

	private Instruction createConstraint(GeneratorContext ctx, ObjectName tableName, Constraint_defContext rule) {
        ObjectName parentTableName = TableName.parse(ctx, rule.table_name_());
        List<String> childColumns = Utils.getColumns(rule.columns(0));
        List<String> parentColumns = Utils.getColumns(rule.columns(1));
		CreateForeignKey fk = new CreateForeignKey(tableName, parentTableName, childColumns, parentColumns);
		return fk;
	}

	private CreateColumn createColumn(GeneratorContext ctx, 
			                          Column_defContext rule, 
			                          ObjectName tableName, 
			                          List<String> keyColumns) {
        CreateColumn createColumn = new CreateColumn();
        createColumn.tableName = tableName;
        Utils.updateColumnAttributes(ctx, createColumn, rule, keyColumns);
        return createColumn;
    }

	private Instruction createPrimrayKey(GeneratorContext ctx, ObjectName tableName, Primary_key_defContext rule) {
        List<String> columns = Utils.getColumns(rule.index_columns());
        CreatePrimaryKey step = new CreatePrimaryKey(tableName, columns);
        return step;
    }

	private Instruction createPrimrayKey(GeneratorContext ctx, ObjectName tableName, Index_defContext rule) {
        List<String> columns = Utils.getColumns(rule.index_columns());
        CreatePrimaryKey step = new CreatePrimaryKey(tableName, columns);
        return step;
    }

    private Instruction createIndex(GeneratorContext ctx, ObjectName tableName, Index_defContext rule) {
    	boolean isUnique = rule.K_UNIQUE() != null;
    	String indexName = Utils.getIdentifier(rule.identifier());
        List<String> columns = Utils.getColumns(rule.index_columns());
        CreateIndex step = new CreateIndex(indexName, isUnique, false, tableName, columns);
        return step;
	}

}
