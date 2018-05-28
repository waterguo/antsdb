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

import com.antsdb.saltedfish.cpp.FishTimestamp;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.sql.DataType;

/**
 * 
 * @author *-xguo0<@
 */
public class OpInterval extends UnaryOperator {
	long multiplier;
	
    public OpInterval(Operator upstream, long multiplier) {
        super(upstream);
        this.multiplier = multiplier;
    }

    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        long pValue = upstream.eval(ctx, heap, params, pRecord);
        if (pValue == 0) {
        	return 0;
        }
        long value = AutoCaster.getLong(pValue);
        value = value * this.multiplier;
        return FishTimestamp.allocSet(heap, value);
    }

    @Override
    public DataType getReturnType() {
        return DataType.timestamp();
    }

	@Override
	public String toString() {
		return "INTERVAL " + this.upstream.toString() + " * " + this.multiplier;
	}

}
