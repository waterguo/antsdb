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

import java.util.Arrays;
import java.util.List;

import com.antsdb.saltedfish.cpp.FishBool;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.sql.DataType;

public class OpNullEqual extends BinaryOperator {
    public OpNullEqual(Operator left, Operator right) {
        super(left, right);
    }

    @Override
    public List<Operator> getChildren() {
        return Arrays.asList(new Operator[]{left, right});
    }

    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        long xAddr = this.left.eval(ctx, heap, params, pRecord);
        long yAddr = this.right.eval(ctx, heap, params, pRecord);
        boolean result ;
        if ((xAddr == 0) && (yAddr == 0)){
        	result = true;
        }
        else {
        	result = AutoCaster.equals(heap, xAddr, yAddr);
        }
        return FishBool.allocSet(heap, result);
    }

    @Override
    public DataType getReturnType() {
        return DataType.bool();
    }

	@Override
	public String toString() {
		return this.left.toString() + " == " + this.right.toString();
	}

}
