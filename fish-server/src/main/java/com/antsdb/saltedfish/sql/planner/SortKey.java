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
package com.antsdb.saltedfish.sql.planner;

import java.util.ArrayList;
import java.util.List;

import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.RuleMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;

/**
 * 
 * @author *-xguo0<@
 */
public class SortKey {
    ColumnMeta column;
    boolean isAsc = true;

    public SortKey(ColumnMeta column, boolean isAsc) {
        this.column = column;
        this.isAsc = isAsc;
    }

    public static List<SortKey> from(List<ColumnMeta> columns) {
        ArrayList<SortKey> result = new ArrayList<>();
        columns.forEach(it -> {
            result.add(new SortKey(it, true));
        });
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof SortKey)) {
            return false;
        }
        SortKey that = (SortKey)obj;
        if (this.column == that.column) {
            return this.isAsc == that.isAsc;
        }
        if ((this.column == null) || (that.column == null)) {
            return false;
        }
        if (this.column.getId() != that.column.getId()) {
            return false;
        }
        return this.isAsc == that.isAsc;
    }
    
    /**
     * check if y follows x
     * @param x
     * @param y
     * @return 1 if follows ascending order, -1 if follows descending order, 0 not following
     */
    public static int follow(List<SortKey> x, List<SortKey> y) {
        if (y.size() > x.size()) {
            return 0;
        }
        int sum = 0;
        for (int i=0; i<y.size(); i++) {
            if (i >= x.size()) {
                return 0;
            }
            SortKey xx = x.get(i);
            SortKey yy = y.get(i);
            if ((xx.column == null) || (yy.column == null)) {
                return 0;
            }
            if (xx.column.getId() != yy.column.getId()) {
                return 0;
            }
            int delta = (xx.isAsc == yy.isAsc) ? 1 : -1;
            sum += delta;
        }
        if (Math.abs(sum) != y.size()) {
            return 0;
        }
        return (sum > 0) ? 1 : -1;
    }

    public static List<SortKey> from(TableMeta table, RuleMeta<?> rule) {
        List<SortKey> result = new ArrayList<>();
        for (ColumnMeta column:rule.getColumns(table)) {
            result.add(new SortKey(column, true));
        }
        return result;
    }
}
