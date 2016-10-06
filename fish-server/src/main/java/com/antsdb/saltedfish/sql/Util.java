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
package com.antsdb.saltedfish.sql;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;

import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.vdm.NullIfEmpty;
import com.antsdb.saltedfish.sql.vdm.Operator;
import com.antsdb.saltedfish.sql.vdm.ToBigDecimal;
import com.antsdb.saltedfish.sql.vdm.ToBytes;
import com.antsdb.saltedfish.sql.vdm.ToDate;
import com.antsdb.saltedfish.sql.vdm.ToDouble;
import com.antsdb.saltedfish.sql.vdm.ToEnumIndex;
import com.antsdb.saltedfish.sql.vdm.ToFloat;
import com.antsdb.saltedfish.sql.vdm.ToInteger;
import com.antsdb.saltedfish.sql.vdm.ToLong;
import com.antsdb.saltedfish.sql.vdm.ToString;
import com.antsdb.saltedfish.sql.vdm.ToTime;
import com.antsdb.saltedfish.util.CodingError;

public class Util {
    public static Operator autoCast(ColumnMeta column, Operator expr, boolean nullIfEmpty) {
        Operator newone;
        if (expr.getReturnType() != null) {
            if (expr.getReturnType().getJavaType() == column.getDataType().getJavaType()) {
                return expr;
            }
        }
        Class<?> type = column.getDataType().getJavaType();
        if (type == String.class) {
            newone = new ToString(expr);
            if (nullIfEmpty) {
            	newone = new NullIfEmpty(newone);
            }
        }
        else if (type == Integer.class) {
        	if (column.getDataType().getName().equals("enum")) {
        		newone = new ToEnumIndex(column, expr);
        	}
        	else {
                newone = new ToInteger(expr);
        	}
        }
        else if (type == Long.class) {
            newone = new ToLong(expr);
        }
        else if (type == Float.class) {
            newone = new ToFloat(expr);
        }
        else if (type == BigDecimal.class) {
            newone = new ToBigDecimal(new NullIfEmpty(expr));
        }
        else if (type == byte[].class) {
            newone = new NullIfEmpty(new ToBytes(expr));
        }
        else if (type == Date.class) {
        	newone = new ToDate(expr);
        }
        else if (type == Double.class) {
        	newone = new ToDouble(expr);
        }
        else if (type == Time.class) {
        	newone = new ToTime(expr);
        }
        else {
            throw new CodingError();
        }
        return newone;
    }
}
