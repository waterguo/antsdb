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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.antsdb.saltedfish.lexer.MysqlParser.Column_defContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Constraint_defContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Create_defContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Create_table_stmtContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Index_defContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Primary_key_defContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Table_optionContext;
import com.antsdb.saltedfish.server.mysql.ErrorMessage;
import com.antsdb.saltedfish.sql.DdlGenerator;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.vdm.Commit;
import com.antsdb.saltedfish.sql.planner.Planner;
import com.antsdb.saltedfish.sql.vdm.CreateColumn;
import com.antsdb.saltedfish.sql.vdm.CreateForeignKey;
import com.antsdb.saltedfish.sql.vdm.CreateIndex;
import com.antsdb.saltedfish.sql.vdm.CreatePrimaryKey;
import com.antsdb.saltedfish.sql.vdm.CreateTable;
import com.antsdb.saltedfish.sql.vdm.CursorMaker;
import com.antsdb.saltedfish.sql.vdm.FieldMeta;
import com.antsdb.saltedfish.sql.vdm.Flow;
import com.antsdb.saltedfish.sql.vdm.IfTableNotExist;
import com.antsdb.saltedfish.sql.vdm.InsertSelectByName;
import com.antsdb.saltedfish.sql.vdm.Instruction;
import com.antsdb.saltedfish.sql.vdm.ObjectName;
import com.antsdb.saltedfish.sql.vdm.PostSchemaChange;
import com.antsdb.saltedfish.util.Pair;

public class Create_table_stmtGenerator extends DdlGenerator<Create_table_stmtContext>{

    @Override
    public boolean isTemporaryTable(GeneratorContext ctx, Create_table_stmtContext rule) {
        return rule.K_TEMPORARY() != null;
    }

    @Override
    public Instruction gen(GeneratorContext ctx, Create_table_stmtContext rule) throws OrcaException {
        Flow flow = new Flow();
        ObjectName tableName = TableName.parse(ctx, rule.table_name_());
        CreateTable createTable = new CreateTable(tableName);
        createTable.setTemporary(rule.K_TEMPORARY() != null);
        flow.add(createTable);
        
        // collect table options
        Map<String, String> options = new HashMap<>();
        for (Table_optionContext i:rule.table_options().table_option()) {
            String option = i.table_option_name().getText();
            String value = i.table_option_value().getText();
            options.put(option, value);
            if (option.equalsIgnoreCase("DEFAULTCHARSET")) {
                createTable.setCharset(value);
            }
            else if (option.equalsIgnoreCase("ENGINE")) {
                createTable.setEngine(value);
            }
            else if (option.equalsIgnoreCase("COMMENT")) {
                createTable.setComment(Utils.strip(value));
            }
        }
        
        // create columns
        CreatePrimaryKey cpk = null;
        HashMap<String, CreateColumn> columnByName = new HashMap<>();
        if (rule.create_defs() != null) {
            for (Create_defContext i:rule.create_defs().create_def()) {
                if (i.column_def() != null) {
                    List<String> keyColumns = new ArrayList<>();
                    CreateColumn cc = createColumn(ctx, i.column_def(), tableName, keyColumns);
                    flow.add(cc);
                    columnByName.put(cc.columnName.toLowerCase(), cc);
                    if (keyColumns.size() > 0) {
                        cpk = new CreatePrimaryKey(tableName, keyColumns);
                        flow.add(cpk);
                    }
                    else if (keyColumns.size() > 1) {
                        throw new OrcaException("Multiple primary key defined");
                    }
                }
            }
        }
        
        // create constraints
        Set<String> pkColumns = new HashSet<>();
        List<CreateIndex> indexes = new ArrayList<>();
        if (rule.create_defs() != null) {
            for (Create_defContext i:rule.create_defs().create_def()) {
                if (i.primary_key_def() != null) {
                    if (cpk != null) {
                        throw new ErrorMessage(42000, "Multiple primary key defined");
                    }
                    cpk = createPrimrayKey(ctx, tableName, i.primary_key_def(), pkColumns);
                    flow.add(cpk);
                }
                else if (i.index_def() != null) {
                    indexes.add(createIndex(ctx, tableName, i.index_def()));
                }
                else if (i.constraint_def() != null) {
                    Instruction step = createConstraint(ctx, tableName, i.constraint_def());
                    flow.add(step);
                }
            }
        }
        
        // convert unique key to primary key if all contributing columns are null and no primary key is defined
        for (CreateIndex i:indexes) {
            boolean hasNull = false;
            if (cpk == null) {
                for (Pair<String, Integer> j: i.columns) {
                    CreateColumn cc = columnByName.get(j.x.toLowerCase());
                    if (cc.isNullable()) {
                        hasNull = true;
                        break;
                    }
                }
            }
            if (cpk!=null || hasNull) {
                flow.add(i);
            }
            else {
                List<String> columns = new ArrayList<>();
                i.columns.forEach((it)->{columns.add(it.x);});
                cpk = new CreatePrimaryKey(tableName, columns); 
                flow.add(cpk);
            }
        }
        
        // making primary key column not null
        if (cpk != null) {
            for (String i:cpk.columns) {
                CreateColumn cc = columnByName.get(i.toLowerCase());
                if (cc != null) {
                    cc.setNullable(false);
                    if (cc.defaultValue == null && !cc.isAutoIncrement()) cc.setDefaultValue("'0'");
                }
            }
        }
        
        // ctas
        if (rule.select_or_values() != null) {
            genSelect(ctx, flow, tableName, rule);
        }
        
        // finishing up
        if (flow.getInstructions().stream().filter((x)->{return x instanceof CreateColumn;}).count() == 0) {
            throw new OrcaException("A table must have at least 1 column");
        }
        flow.add(new SyncTableSequence(tableName, options));
        flow.add(new Commit());
        flow.add(new PostSchemaChange(tableName));
        if (rule.K_NOT() == null) {
            return flow;
        }
        else {
            return new IfTableNotExist(tableName, flow);
        }
    }

    private void genSelect(GeneratorContext ctx, 
                           Flow flow, 
                           ObjectName tableName, 
                           Create_table_stmtContext rule) {
        Planner planner = Select_or_valuesGenerator.gen(ctx, rule.select_or_values(), null);
        CursorMaker maker = planner.run();
        for (FieldMeta i:maker.getCursorMeta().getColumns()) {
            boolean foundMatch = false;
            for (Instruction j:flow.getInstructions()) {
                if (j instanceof CreateColumn) {
                    CreateColumn jj = (CreateColumn) j;
                    if (jj.getColumnName().equalsIgnoreCase(i.getName())) {
                        foundMatch = true;
                        break;
                    }
                }
            }
            if (!foundMatch) {
                CreateColumn cc = new CreateColumn();
                cc.tableName = tableName;
                cc.columnName = i.getName();
                cc.type = ctx.getOrca().getTypeFactory().findDefaultType(i.getType().getJavaType());
                flow.add(cc);
            }
        }
        flow.add(new InsertSelectByName(tableName, maker));
    }

    private Instruction createConstraint(GeneratorContext ctx, ObjectName tableName, Constraint_defContext rule) {
        ObjectName parentTableName = TableName.parse(ctx, rule.table_name_());
        List<String> childColumns = Utils.getColumns(rule.columns(0));
        List<String> parentColumns = Utils.getColumns(rule.columns(1));
        String name = (rule.identifier() == null) ? null : Utils.getIdentifier(rule.identifier());
        String onDelete = Alter_table_stmtGenerator.getOnAction(true, rule.cascade_option());
        String onUpdate = Alter_table_stmtGenerator.getOnAction(false, rule.cascade_option());
        CreateForeignKey fk;
        fk = new CreateForeignKey(
                tableName, 
                name, 
                null, 
                parentTableName, 
                childColumns, 
                parentColumns, 
                onDelete, 
                onUpdate);
        fk.setRebuildIndex(false);
        return fk;
    }

    private CreateColumn createColumn(GeneratorContext ctx, 
                                      Column_defContext rule, 
                                      ObjectName tableName, 
                                      List<String> keyColumns) {
        CreateColumn createColumn = new CreateColumn();
        createColumn.tableName = tableName;
        Utils.updateColumnAttributes(ctx, createColumn, rule, keyColumns);
        /*
        if (pkColumns.contains(createColumn.columnName.toLowerCase())) {
            createColumn.setNullable(false);
            if (createColumn.getDefaultValue() == null && !createColumn.isAutoIncrement()) {
                createColumn.setDefaultValue("'0'");
            }
        }
        */
        return createColumn;
    }

    private CreatePrimaryKey createPrimrayKey(
            GeneratorContext ctx, 
            ObjectName tableName, 
            Primary_key_defContext rule, 
            Set<String> pkColumns) {
        List<String> columns = Utils.getColumns(rule.index_columns());
        for (String i:columns) {
            pkColumns.add(i.toLowerCase());
        }
        CreatePrimaryKey step = new CreatePrimaryKey(tableName, columns);
        return step;
    }

    private CreateIndex createIndex(GeneratorContext ctx, ObjectName tableName, Index_defContext rule) {
        boolean isUnique = rule.K_UNIQUE() != null;
        String indexName = null;
        List<Pair<String, Integer>> columns = Utils.getIndexColumns(rule.index_columns());
            if (rule.identifier() != null) {
                indexName = Utils.getIdentifier(rule.identifier());
            }
            else {
                indexName = columns.get(0).x; 
            }
        boolean isFullText = rule.K_FULLTEXT() != null;
        CreateIndex step = new CreateIndex(indexName, isFullText, isUnique, false, tableName, columns);
        step.setRebuild(false);
        return step;
    }

}
