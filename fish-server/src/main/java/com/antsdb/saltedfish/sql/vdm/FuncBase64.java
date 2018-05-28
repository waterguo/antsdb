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

import java.util.Base64;

import com.antsdb.saltedfish.cpp.Bytes;
import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.sql.DataType;

/**
 * base64 encoding 
 * @author wgu0
 */
public class FuncBase64 extends Function {
	@Override
	public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        long pBytes = this.parameters.get(0).eval(ctx, heap, params, pRecord);
        if (pBytes == 0) {
        	return 0;
        }
        byte[] bytes = Bytes.get(heap, pBytes);
        String result = Base64.getEncoder().encodeToString(bytes);
        long pResult = FishObject.allocSet(heap, result);
        return pResult;
	}

	@Override
	public DataType getReturnType() {
		return DataType.varchar();
	}

	@Override
	public int getMinParameters() {
		return 1;
	}

}
