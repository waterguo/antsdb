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

import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.util.TypeSafeUtil;

/**
 * 
 * @author wgu0
 */
public class FuncMin extends Function {
	int variableId;
	
    private static class Data {
    	Object min;
    }
    
    public FuncMin(int variableId) {
    	this.variableId = variableId;
    }
    
    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
    	Data data = (Data)ctx.getVariable(this.variableId);
    	if (data == null) {
    		data = new Data();
    		ctx.setVariable(variableId, data);
    	}
        if (Record.isGroupEnd(pRecord)) {
            data.min = null;
            return 0;
        }
        long addrVal = this.parameters.get(0).eval(ctx, heap, params, pRecord);
        Object val = FishObject.get(heap, addrVal);
        if (data.min == null) {
            data.min = val;
        }
        else {
            int result = TypeSafeUtil.compare(data.min, val);
            if (result > 0) {
                data.min = val;
            }
        }
        return FishObject.allocSet(heap, data.min);
    }

    @Override
    public DataType getReturnType() {
        return this.parameters.get(0).getReturnType();
    }

	@Override
	public int getMinParameters() {
		return 1;
	}

}
