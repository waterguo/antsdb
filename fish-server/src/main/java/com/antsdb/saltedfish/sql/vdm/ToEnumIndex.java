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

import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.Int4;
import com.antsdb.saltedfish.cpp.Value;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;

/**
 * 
 * convert a string to an enum index
 * 
 * http://dev.mysql.com/doc/refman/5.7/en/enum.html
 * 
 * @author wgu0
 */
public class ToEnumIndex extends UnaryOperator {
    ColumnMeta column;
    Map<String, Integer> indexByEnum;
    
    public ToEnumIndex(ColumnMeta column, Operator expr) {
        super(expr);
        this.column = column;
        this.indexByEnum = column.getEnumValueMap();
        if (this.indexByEnum == null) {
            throw new OrcaException("enum values is not found for column {}", column.getColumnName());
        }
    }

    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        long pValue = this.upstream.eval(ctx, heap, params, pRecord);
        int result;
        int format = Value.getFormat(heap, pValue);
        if (pValue == 0) {
            return 0;
        }
        else if (format == Value.FORMAT_INT4) {
            return pValue;
        }
        else {
            String value = (String)FishObject.get(heap, FishObject.toString(heap, pValue));
            if (StringUtils.isEmpty(value)) {
                result = 0;
            }
            else {
                Integer index = this.indexByEnum.get(value);
                if (index == null) {
                    throw new OrcaException("invalid enum value {}", value);
                }
                result = index;
            }
        }
        return Int4.allocSet(heap, result);
    }

    @Override
    public DataType getReturnType() {
        return DataType.integer();
    }

}
