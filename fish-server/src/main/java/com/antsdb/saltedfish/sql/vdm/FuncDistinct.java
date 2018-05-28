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

import java.util.HashSet;

import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.sql.DataType;

/**
 * mysql count distinct
 * 
 * @see <a href="https://dev.mysql.com/doc/refman/5.0/en/group-by-functions.html#function_count-distinct">count distinct</a>
 * 
 * @author xguo
 *
 */
public class FuncDistinct extends UnaryOperator {
    int variableId;
    
    private static class Data {
        HashSet<Object> values = new HashSet<Object>();
    }
    
    public FuncDistinct(Operator expr, int variableId) {
        super(expr);
        this.variableId = variableId;
    }

    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
    	Data data = (Data)ctx.getVariable(this.variableId);
    	if (data == null) {
    		data = new Data();
    		ctx.setVariable(variableId, data);
    	}
    	
        // end of group by, reset
        
        if (Record.isGroupEnd(pRecord)) {
            data.values.clear();
            this.upstream.eval(ctx, heap, params, pRecord);
            return 0;
        }
        
        // check uniqueness
        
        long addrValue = this.upstream.eval(ctx, heap, params, pRecord);
        Object value = FishObject.get(heap, addrValue);
        if (data.values.contains(value)) {
            return 0;
        }
        else {
            data.values.add(value);
            return addrValue;
        }
    }

    @Override
    public DataType getReturnType() {
        return this.upstream.getReturnType();
    }
    
}
