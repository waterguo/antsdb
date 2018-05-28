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
package com.antsdb.saltedfish.sql.vdm;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.List;

import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.Util;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;

/**
 * bunch of methods dealing with types
 *  
 * @author wgu0
 */
public class TypeUtil {
    public static void applyCasting(List<ColumnMeta> columns, List<Operator> exprs, boolean nullIfEmpty) {
        for (int i=0; i<exprs.size(); i++) {
            ColumnMeta column = columns.get(i);
            Operator expr = exprs.get(i);
            Operator newone = autoCast(column, expr, nullIfEmpty);
            exprs.set(i, newone);
        }
    }

    public static Operator autoCast(ColumnMeta column, Operator expr, boolean nullIfEmpty) {
        if (expr.getReturnType() != null) {
            if (expr.getReturnType().getJavaType() == column.getDataType().getJavaType()) {
                return expr;
            }
        }
        DataType dtype = column.getDataType();
        Class<?> type = dtype.getJavaType();
        if (type == Timestamp.class) {
            return new ToTimestampRelaxed(new NullIfEmpty(expr));
        }
        else if (type == BigDecimal.class) {
            return new ToBigDecimal(new NullIfEmpty(expr), dtype.getLength(), dtype.getScale());
        }
        else {
            return Util.autoCast(column, expr, nullIfEmpty);
        }
    }
}
