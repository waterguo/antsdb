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
package com.antsdb.saltedfish.sql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;

import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.RowIterator;
import com.antsdb.saltedfish.sql.meta.ColumnId;
import com.antsdb.saltedfish.sql.meta.TableId;
import com.antsdb.saltedfish.sql.planner.SortKey;
import com.antsdb.saltedfish.sql.vdm.CursorMeta;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.sql.vdm.View;
import com.antsdb.saltedfish.util.CursorUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class SysRule extends View {
    static CursorMeta META = CursorUtil.toMeta(Line.class);
    
    public static class Line {
        public Integer id;
        public String namespace;
        public Integer table_id;
        public String rule_name;
        public Integer rule_type;
        public Integer index_table_id;
        public String index_table_external_name;
        public Boolean is_fulltext;
        public String columns;
        public String parent_table_name;
        public String parent_column_names;
        public String on_delete;
        public String on_update;
    }
    
    SysRule(Orca orca) {
        super(META);
    }
    
    @Override
    public CursorMeta getCursorMeta() {
        return this.meta;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        GTable table = ctx.getHumpback().getTable(TableId.SYSRULE);
        List<Line> result = new ArrayList<>();
        for (RowIterator i=table.scan(0, Long.MAX_VALUE, true);;) {
            if (!i.next()) {
                break;
            }
            Row row = i.getRow();
            result.add(toLine(row));
        }
        return CursorUtil.toCursor(META, result);
    }

    @Override
    public boolean setSortingOrder(List<SortKey> order) {
        return false;
    }

    private Line toLine(Row row) {
        Line line = new Line();
        line.id = (Integer)row.get(ColumnId.sysrule_id.getId());
        line.namespace = (String)row.get(ColumnId.sysrule_namespace.getId());
        line.table_id = (Integer)row.get(ColumnId.sysrule_table_id.getId());
        line.rule_name = (String)row.get(ColumnId.sysrule_rule_name.getId());
        line.rule_type = (Integer)row.get(ColumnId.sysrule_rule_type.getId());
        line.index_table_id = (Integer)row.get(ColumnId.sysrule_index_table_id.getId());
        line.index_table_external_name = (String)row.get(ColumnId.sysrule_index_external_name.getId());
        line.is_fulltext = (Boolean)row.get(ColumnId.sysrule_is_fulltext.getId());
        line.columns = toString((int[])row.get(ColumnId.sysrule_columns.getId()));
        line.parent_table_name = (String)row.get(ColumnId.sysrule_parent_table_name.getId());
        line.parent_column_names = (String)row.get(ColumnId.sysrule_parent_column_names.getId());
        line.on_delete = (String)row.get(ColumnId.sysrule_on_delete.getId());
        line.on_update = (String)row.get(ColumnId.sysrule_on_update.getId());
        return line;
    }

    private String toString(int[] value) {
        if (value == null) {
            return null;
        }
        return StringUtils.join(Arrays.stream(value).boxed().collect(Collectors.toList()), ",");
    }

}
