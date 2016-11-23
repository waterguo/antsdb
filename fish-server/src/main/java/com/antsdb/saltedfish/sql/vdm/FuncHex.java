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

import com.antsdb.saltedfish.cpp.Bytes;
import com.antsdb.saltedfish.cpp.FishUtf8;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.sql.DataType;

/**
 * 
 * @author wgu0
 */
public class FuncHex extends Function {
	final char[] _hexArray = {'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F'};
    	
	@Override
	public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
		long pValue = AutoCaster.toBytes(heap, getUpstream().eval(ctx, heap, params, pRecord));
		if (pValue == 0) {
			return 0;
		}
		int len = Bytes.getLength(pValue);
		long pResult = FishUtf8.alloc(heap, len * 2);
		for (int i=0; i<len; i++) {
			int value = Unsafe.getByte(pValue + 4 + i) & 0xff;
			Unsafe.putByte(pResult + 4 + i * 2, (byte)_hexArray[value >>> 4]);
			Unsafe.putByte(pResult + 4 + i * 2 + 1, (byte)_hexArray[value & 0xf]);
		}
		return pResult;
	}

	@Override
	public DataType getReturnType() {
		return DataType.varchar();
	}
	
	public Operator getUpstream() {
		return this.parameters.get(0);
	}

	@Override
	public int getMinParameters() {
		return 1;
	}

}
