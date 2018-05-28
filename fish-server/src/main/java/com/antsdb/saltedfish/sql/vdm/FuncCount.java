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

import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.Int8;
import com.antsdb.saltedfish.sql.DataType;

public class FuncCount extends Function {
	int variableId;

	private static class Data {
		long counter = 0;
	}
	
    public FuncCount(int variableId) {
        this.variableId = variableId;
    }

    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
    	// initialize
    	
    	Data data = (Data)ctx.getVariable(this.variableId);
    	if (data == null) {
    		data = new Data();
    		ctx.setVariable(variableId, data);
    	}
    	
    	// logic to deal with group end
    	
        if (Record.isGroupEnd(pRecord)) {
            data.counter = 0;
            if (this.getParameter() != null) {
                // pass the group end marker to deeper level
                this.getParameter().eval(ctx, heap, params, pRecord);
            }
            return Int8.allocSet(heap, 0);
        }
        
        // if this is a count(*)
        
        if (this.getParameter() == null) {
            if (pRecord != 0) {
                data.counter++;
            }
        }
        
        // if this is a count(<expression>), do a null check 
        
        else {
            long addrValue = this.getParameter().eval(ctx, heap, params, pRecord);
            if (addrValue != 0) {
                data.counter++;
            }
        }
        return Int8.allocSet(heap, data.counter);
    }

    @Override
    public DataType getReturnType() {
        return DataType.longtype();
    }

    Operator getParameter() {
        return this.parameters.get(0);
    }

	@Override
	public int getMinParameters() {
		return 1;
	}
}
