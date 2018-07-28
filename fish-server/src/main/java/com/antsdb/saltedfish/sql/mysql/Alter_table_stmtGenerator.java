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
import java.util.List;

import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;

import com.antsdb.saltedfish.lexer.MysqlParser.Alter_table_add_constraintContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Alter_table_add_constraint_fkContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Alter_table_add_constraint_pkContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Alter_table_add_indexContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Alter_table_add_primary_keyContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Alter_table_dropContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Alter_table_drop_indexContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Alter_table_modifyContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Alter_table_optionsContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Alter_table_renameContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Alter_table_stmtContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Cascade_optionContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Column_defContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Column_def_setContext;
import com.antsdb.saltedfish.sql.DdlGenerator;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.vdm.Commit;
import com.antsdb.saltedfish.sql.vdm.CreateColumn;
import com.antsdb.saltedfish.sql.vdm.CreateForeignKey;
import com.antsdb.saltedfish.sql.vdm.CreateIndex;
import com.antsdb.saltedfish.sql.vdm.CreatePrimaryKey;
import com.antsdb.saltedfish.sql.vdm.DeleteColumn;
import com.antsdb.saltedfish.sql.vdm.DropIndex;
import com.antsdb.saltedfish.sql.vdm.Flow;
import com.antsdb.saltedfish.sql.vdm.Instruction;
import com.antsdb.saltedfish.sql.vdm.ModifyColumn;
import com.antsdb.saltedfish.sql.vdm.MysqlUpgradeIndexToPrimaryKey;
import com.antsdb.saltedfish.sql.vdm.ObjectName;
import com.antsdb.saltedfish.sql.vdm.RunScript;
import com.antsdb.saltedfish.util.UberUtil;

public class Alter_table_stmtGenerator extends DdlGenerator<Alter_table_stmtContext> {
    static Logger _log = UberUtil.getThisLogger();
    
    @Override
    public Instruction gen(GeneratorContext ctx, Alter_table_stmtContext rule)
    throws OrcaException {
        Flow flow = new Flow();
        ObjectName tableName = TableName.parse(ctx, rule.table_name_());
        List<Alter_table_optionsContext> options = rule.alter_table_options();
        options.forEach(
            it -> 
            {
                if (it.alter_table_add_constraint() != null) {
                    flow.add(createAddConstraint(ctx, tableName, it.alter_table_add_constraint()));
                }
                else if (it.alter_table_add_primary_key() != null) {
                    flow.add(createAddPrimaryKey(ctx, tableName, it.alter_table_add_primary_key()));
                }
                else if (it.alter_table_modify() != null) {
                    flow.add(modifyColumn(ctx, tableName, it.alter_table_modify()));
                }
                else if (it.alter_table_rename() != null) {
                    flow.add(renameColumn(ctx, tableName, it.alter_table_rename()));
                }
                else if (it.alter_table_add() != null) {
                    Column_def_setContext colSet = it.alter_table_add().column_def_set();
                    if (colSet.column_def() != null) {
                        createColumn(flow, ctx, colSet.column_def(), tableName);
                    }
                    else if (colSet.column_def_list() != null)
                    {
                        colSet.column_def_list().column_def().forEach( itdef -> {
                            createColumn(flow, ctx, itdef, tableName);
                        });
                    }
                }
                else if (it.alter_table_drop() != null) {
                    flow.add(dropColumn(ctx, it.alter_table_drop(), tableName));
                }
                else if (it.alter_table_disable_keys() != null) {
                    _log.warn("ALTER TABLE DISABLE KEYS has not been implemented");
                }
                else if (it.alter_table_enable_keys() != null) {
                    _log.warn("ALTER TABLE ENABLE KEYS has not been implemented");
                }
                else if (it.alter_table_add_index() != null) {
                    flow.add(addIndex(ctx, it.alter_table_add_index(), tableName));
                }
                else if (it.alter_table_drop_index() != null) {
                    flow.add(dropIndex(ctx, it.alter_table_drop_index(), tableName));
                }
                else {
                    throw new NotImplementedException();
                }
            }
        );
                
                
        flow.add(new SyncTableSequence(tableName, null));
        return flow;
    }

    private Instruction dropIndex(GeneratorContext ctx, Alter_table_drop_indexContext rule, ObjectName tableName) {
        String indexName = Utils.getIdentifier(rule.identifier());
        Instruction result = new DropIndex(tableName, indexName);
        return result;
    }

    private Instruction addIndex(GeneratorContext ctx, Alter_table_add_indexContext rule, ObjectName tableName) {
        List<String> columns = new ArrayList<String>();
        rule.indexed_column_def().forEach((it) -> columns.add(Utils.getIdentifier(it.indexed_column().identifier())));
        String indexName = null;
        if (rule.index_name() != null) {
            indexName = Utils.getIdentifier(rule.index_name().identifier());
        }
        else {
            indexName = columns.get(0); 
        }
        boolean isUnique = rule.K_UNIQUE() != null;
        boolean isFullText = rule.K_FULLTEXT() != null;
        Instruction result = new CreateIndex(indexName, isFullText, isUnique, false, tableName, columns);
        if (isUnique) {
            Flow flow = new Flow();
            flow.add(result);
            flow.add(new MysqlUpgradeIndexToPrimaryKey(tableName, indexName));
            result = flow;
        }
        return result;
    }

    private Instruction dropColumn(GeneratorContext ctx, Alter_table_dropContext rule, ObjectName tableName) {
        String columnName = Utils.getIdentifier(rule.identifier());
        DeleteColumn dc = new DeleteColumn(tableName, columnName);
        return dc;
    }

    private CreateColumn createColumn(Flow flow, GeneratorContext ctx, Column_defContext rule, ObjectName tableName) {
        CreateColumn cc = new CreateColumn();
        cc.tableName = tableName;
        List<String> pkColumns = new ArrayList<>();
        Utils.updateColumnAttributes(ctx, cc, rule, pkColumns);
        flow.add(cc);
        if (cc.defaultValue != null) {
            String sql = String.format("UPDATE %s SET %s=%s", tableName, cc.columnName, cc.defaultValue);
            flow.add(new RunScript(sql, new Object[0]));
            flow.add(new Commit());
        }
        if (rule.K_AFTER() != null) {
            String after = Utils.getIdentifier(rule.identifier());
            cc.setAfter(after);
        }
        if (pkColumns.size() != 0) {
            CreatePrimaryKey cpk = new CreatePrimaryKey(tableName, pkColumns);
            flow.add(cpk);
        }
        return cc;
    }


    private Instruction renameColumn(GeneratorContext ctx, ObjectName tableName, Alter_table_renameContext rule) {
        String oldColumnName;
        if (rule.identifier() != null) {
            oldColumnName = Utils.getIdentifier(rule.identifier());
        }
        else {
            oldColumnName = Utils.getIdentifier(rule.column_def().column_name().identifier());
        }
        return modifyColumn(ctx, tableName, oldColumnName, rule.column_def());
    }
    
    private Instruction modifyColumn(GeneratorContext ctx, ObjectName tableName, Alter_table_modifyContext rule) {
        String oldColumnName = Utils.getIdentifier(rule.column_def().column_name().identifier());
        return modifyColumn(ctx, tableName, oldColumnName, rule.column_def());
    }

    private Instruction modifyColumn(
            GeneratorContext ctx, 
            ObjectName tableName, 
            String oldName, 
            Column_defContext rule) {
        ModifyColumn mc = new ModifyColumn(tableName, oldName);
        Utils.updateColumnAttributes(ctx, mc, rule, new ArrayList<String>());
        return mc;
    }

    private Instruction createAddPrimaryKey(
            GeneratorContext ctx, 
            ObjectName tableName, 
            Alter_table_add_primary_keyContext rule) {
        List<String> columns = Utils.getColumns(rule.columns());
        return new CreatePrimaryKey(tableName, columns);
    }

    private Instruction createAddConstraint(
            GeneratorContext ctx, 
            ObjectName tableName, 
            Alter_table_add_constraintContext rule) {
        if (rule.alter_table_add_constraint_fk() != null) {
            String name = (rule.any_name() != null) ? rule.any_name().getText() : null;
            return createAddForeignKey(ctx, tableName, name, rule.alter_table_add_constraint_fk());
        }
        else if (rule.alter_table_add_constraint_pk() != null) {
            return createAddPrimaryKey(ctx, tableName, rule.alter_table_add_constraint_pk());
        }
        else {
            throw new NotImplementedException();
        }
    }

    private Instruction createAddPrimaryKey(
            GeneratorContext ctx, 
            ObjectName tableName,
            Alter_table_add_constraint_pkContext rule) {
        List<String> columns = Utils.getColumns(rule.columns());
        return new CreatePrimaryKey(tableName, columns);
    }

    private Instruction createAddForeignKey(
            GeneratorContext ctx, 
            ObjectName tableName, 
            String name,
            Alter_table_add_constraint_fkContext rule) {
        ObjectName parentTable = TableName.parse(ctx, rule.table_name_());
        List<String> childColumns = Utils.getColumns(rule.child_columns().columns());
        List<String> parentColumns = Utils.getColumns(rule.parent_columns().columns());
        String onDelete = getOnAction(true, rule.cascade_option());
        String onUpdate = getOnAction(false, rule.cascade_option());
        return new CreateForeignKey(tableName, name, parentTable, childColumns, parentColumns, onDelete, onUpdate);
    }

    static private String getAction(Cascade_optionContext rule) {
        String result = "";
        for (int i=2; i<rule.getChildCount(); i++) {
            if (i > 2) {
                result += " ";
            }
            result += rule.getChild(i).getText();
        }
        return result;
    }

    static String getOnAction(boolean isDelete, List<Cascade_optionContext> options) {
        if (options == null) {
            return null;
        }
        String result = null;
        for (Cascade_optionContext i:options) {
            if (isDelete && i.K_DELETE() != null) {
                result = getAction(i);
            }
            else if (i.K_UPDATE() != null) {
                result = getAction(i);
            }
        }
        return result;
    }
}
