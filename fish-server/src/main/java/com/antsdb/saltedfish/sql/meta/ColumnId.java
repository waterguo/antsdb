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
package com.antsdb.saltedfish.sql.meta;

public enum ColumnId {
    /* systable */
    systable_id(1),
    systable_namespace(2),
    systable_table_name(3),
    systable_table_type(4),
    systable_ext_name(5),
    systable_htable_id(6),
    systable_charset(7),
    systable_engine(8),
    systable_sql(9),
    systable_comment(10),
    systable_end_of_columns(11),
    /* syscolumn */
    syscolumn_id(1),
    syscolumn_column_id(2),
    syscolumn_namespace(3),
    syscolumn_table_name(4),
    syscolumn_column_name(5), 
    syscolumn_type_name(6), 
    syscolumn_type_length(7),
    syscolumn_type_scale(8), 
    syscolumn_nullable(9),
    syscolumn_default_value(10),
    syscolumn_time_id(11),
    syscolumn_auto_increment(12),
    syscolumn_collation(13),
    syscolumn_enum_values(14),
    syscolumn_seq(15),
    syscolumn_table_id(16),
    syscolumn_zerofill(17),
    syscolumn_end_of_columns(18),
    /* syssequence */
    syssequence_id(1),
    syssequence_namespace(2),
    syssequence_sequence_name(3),
    syssequence_last_number(4),
    syssequence_seed(5),
    syssequence_increment(6),
    syssequence_end_of_columns(7),
    /* sysrule */
    sysrule_id(1),
    sysrule_namespace(2),
    sysrule_table_id(3),
    sysrule_rule_name(4),
    sysrule_rule_type(5),
    sysrule_is_unique(6),
    sysrule_index_table_id(7),
    sysrule_index_external_name(8),
    sysrule_parent_table_id(9), // deprecated, use sysrule_parent_table_name
    sysrule_is_fulltext(10),
    sysrule_columns(11),
    sysrule_parent_columns(12), // deprecated, use sysrule_parent_column_names
    sysrule_parent_table_name(13),
    sysrule_parent_column_names(14),
    sysrule_on_delete(15),
    sysrule_on_update(16),
    sysrule_index_prefix(17),
    sysrule_end_of_columns(18),
    /* sysuser */
    sysuser_id(1),
    sysuser_name(2),
    sysuser_password(3),
    sysuser_auth_type(4),
    sysuser_delete_mark(5),
    sysuser_end_of_columns(6),
    ;
    
    private final int id;
    
    private ColumnId(int id) {
        this.id = id;
    }
    
    public int getId() {
        return this.id;
    }
    
    public static ColumnId valueOf(String table, int id) {
        for (ColumnId i:ColumnId.values()) {
            if (i.toString().startsWith(table) && (i.id == id)) {
                return i;
            }
        }
        return null;
    }
    
    public static ColumnId valueOf(String table, String column) {
        String key = table + "_" + column;
        key = key.toLowerCase();
        ColumnId id = ColumnId.valueOf(key);
        return id;
    }
    
    public static String getFieldName(int id) {
        id = -id;
        for (ColumnId i:ColumnId.values()) {
            if (i.ordinal() == id) {
                return i.name();
            }
        }
        return null;
    }
}
