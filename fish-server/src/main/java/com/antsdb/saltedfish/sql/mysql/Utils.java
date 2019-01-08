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

import com.antsdb.saltedfish.lexer.MysqlParser.Column_constraintContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Column_constraint_defaultContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Column_constraint_nullableContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Column_defContext;
import com.antsdb.saltedfish.lexer.MysqlParser.ColumnsContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Data_typeContext;
import com.antsdb.saltedfish.lexer.MysqlParser.IdentifierContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Index_columnsContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Literal_valueContext;
import com.antsdb.saltedfish.lexer.MysqlParser.String_valueContext;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.DataTypeFactory;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.planner.Planner;
import com.antsdb.saltedfish.sql.vdm.ColumnAttributes;
import com.antsdb.saltedfish.sql.vdm.ExprUtil;
import com.antsdb.saltedfish.sql.vdm.Operator;
import com.antsdb.saltedfish.sql.vdm.TypeUtil;
import com.antsdb.saltedfish.util.Pair;
import com.antsdb.saltedfish.util.UberUtil;

class Utils {
    static Logger _log = UberUtil.getThisLogger();

    static List<String> getColumns(ColumnsContext columns) {
        List<String> list = new ArrayList<>();
        columns.identifier().forEach(it -> list.add(Utils.getIdentifier(it)));
        return list;
    }

    public static List<String> getColumns(Index_columnsContext columns) {
        List<String> list = new ArrayList<>();
        columns.index_column().forEach(it -> {
            list.add(Utils.getIdentifier(it.identifier()));
        });
        return list;
    }

    public static List<Pair<String, Integer>> getIndexColumns(Index_columnsContext columns) {
        List<Pair<String, Integer>> list = new ArrayList<>();
        columns.index_column().forEach(it -> {
            Pair<String, Integer> pair = new Pair<>();
            pair.x = Utils.getIdentifier(it.identifier());
            if (it.signed_number() != null) {
                pair.y = Integer.parseInt(it.signed_number().getText());
            }
            list.add(pair);
        });
        return list;
    }

    static String getIdentifier(IdentifierContext id) {
        if (id == null) {
            return null;
        }
        String s = id.getText();
        if (id.BACKTICK_QUOTED_IDENTIFIER() != null) {
            return s.substring(1, s.length() - 1);
        }
        else if (id.DOUBLE_QUOTED_LITERAL() != null) {
            return s.substring(1, s.length() - 1);
        }
        else {
            return s;
        }
    }

    static String getLiteralValue(String_valueContext rule) {
        if (rule == null) {
            return null;
        }
        String value = rule.getText();
        value = value.substring(1, value.length() - 1);
        return value;
    }

    static void applyCasting(List<ColumnMeta> columns, List<Operator> exprs) {
        TypeUtil.applyCasting(columns, exprs, false);
    }

    static Operator autoCast(ColumnMeta column, Operator expr) {
        return TypeUtil.autoCast(column, expr, false);
    }

    static void updateColumnAttributes(
            GeneratorContext ctx, 
            ColumnAttributes attrs, 
            Column_defContext rule,
            List<String> keyColumns) {
        String columnName = getIdentifier(rule.column_name().identifier());
        DataType type = parse(ctx.getTypeFactory(), rule.data_type());
        type.setZeroFill(rule.K_ZEROFILL() != null);
        attrs.setColumnName(columnName).setType(type).setNullable(true).setAutoIncrement(false);
        if (type.getName().equals("enum")) {
            String values = rule.data_type().enum_type().enum_type_value().getText();
            values = values.substring(1, values.length() - 1);
            attrs.setEnumValues(values);
        }
        for (Column_constraintContext i : rule.column_constraint()) {
            if (i.column_constraint_nullable() != null) {
                setColumnNullable(attrs, i.column_constraint_nullable());
            }
            else if (i.column_constraint_default() != null) {
                setColumnDefault(attrs, i.column_constraint_default());
            }
            else if (i.column_constraint_auto_increment() != null) {
                attrs.setAutoIncrement(true);
            }
            else if (i.column_constraint_primary_key() != null) {
                attrs.setNullable(false);
                keyColumns.add(columnName);
            }
            else if (i.column_constraint_collate() != null) {
                String collate = i.column_constraint_collate().any_name().getText();
                if (collate.equalsIgnoreCase("utf8_bin")) {
                }
                else if (collate.equalsIgnoreCase("utf8_unicode_ci")) {
                }
                else {
                    _log.warn("column collate {} is ignored for column {}", collate, columnName);
                }
            }
            else if (i.column_constraint_character_set() != null) {
                _log.warn("character set is ignored for column {}", columnName);
            }
            else {
                throw new NotImplementedException(i.getText());
            }
        }

    }

    private static void setColumnNullable(ColumnAttributes attrs, Column_constraint_nullableContext rule) {
        attrs.setNullable(rule.K_NOT() == null);
    }

    private static void setColumnDefault(ColumnAttributes attrs, Column_constraint_defaultContext rule) {
        if (rule.literal_value() != null) {
            String value = rule.literal_value().getText();
            if (value.equalsIgnoreCase("null")) {
                attrs.setDefaultValue(null);
            }
            else {
                attrs.setDefaultValue(value);
            }
        }
        else if (rule.signed_number() != null) {
            attrs.setDefaultValue(rule.signed_number().getText());
        }
        else {
            throw new NotImplementedException();
        }
    }

    public static DataType parse(DataTypeFactory fac, Data_typeContext rule) {
        String typeName = null;
        int length = 0;
        int scale = 0;
        if (rule.data_type_nothing() != null) {
            typeName = rule.data_type_nothing().any_name().getText();
        }
        else if (rule.data_type_length() != null) {
            typeName = rule.data_type_length().any_name().getText();
            length = Integer.parseInt(rule.data_type_length().signed_number().getText());
        }
        else if (rule.data_type_length_scale() != null) {
            typeName = rule.data_type_length_scale().any_name().getText();
            length = Integer.parseInt(rule.data_type_length_scale().signed_number(0).getText());
            scale = Integer.parseInt(rule.data_type_length_scale().signed_number(1).getText());
        }
        else if (rule.enum_type() != null) {
            typeName = rule.enum_type().any_name().getText();
        }
        if (rule.K_UNSIGNED() != null) {
            typeName += " unsigned";
        }
        return fac.newDataType(typeName, length, scale);
    }

    static Operator findInPlannerOutputFields(Planner planner, String name) {
        if (name.startsWith("`") || name.startsWith("\"")) {
            name = name.substring(1, name.length() - 1);
        }
        return planner.findOutputField(name);
    }

    static String getQuotedLiteralValue(Literal_valueContext rule) {
        if (rule == null) {
            return null;
        }
        String value = rule.getText();
        value = value.substring(1, value.length() - 1);
        value = ExprUtil.unescape(value);
        return value;
    }
    
    static String getQuotedLiteralValue(String_valueContext rule) {
        if (rule == null) {
            return null;
        }
        String value = rule.getText();
        value = value.substring(1, value.length() - 1);
        value = ExprUtil.unescape(value);
        return value;
    }
}
