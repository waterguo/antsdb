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
package com.antsdb.saltedfish.sql.vdm;

import java.util.function.Consumer;

import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.sql.DataType;

/**
 * this is mysql specific
 * 
 * @author xinyi
 *
 */
public class SystemVariableValue extends Operator {
    String name;
    
    public SystemVariableValue(String name) {
        super();
        this.name = name;
    }

    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
    	String name = this.name.substring(2, this.name.length());
    	Object value;
    	if (name.startsWith("global.")) {
            name = name.substring(7);
            value = ctx.getOrca().getDefaultSession().getParameters().get(name);
    	}
    	else if (name.startsWith("session.")) {
    	    name = name.substring(8);
            value = ctx.getSession().getParameters().get(name);
    	}
    	else {
            value = ctx.getSession().getParameters().get(name);
    	}
        return FishObject.allocSet(heap, value);
    }

    @Override
    public DataType getReturnType() {
        return DataType.varchar();
    }

    @Override
    public void visit(Consumer<Operator> visitor) {
        visitor.accept(this);
    }
}
