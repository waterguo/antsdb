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
package com.antsdb.saltedfish.cpp;

import java.math.BigDecimal;
import java.math.BigInteger;

public final class FishDecimal {

	final static int compare(long addrx, byte typey, long addry) {
		BigDecimal x = get(null, addrx);
		BigDecimal y;
		switch (typey) {
		case Value.FORMAT_DECIMAL:
			y = get(null, addry);
			break;
		case Value.FORMAT_FAST_DECIMAL:
			y = FastDecimal.get(null, addry);
			break;
		case Value.FORMAT_INT8:
			y = BigDecimal.valueOf(Int8.get(null, addry));
			break;
		case Value.FORMAT_INT4:
			y = BigDecimal.valueOf(Int4.get(addry));
			break;
		default:
			throw new IllegalArgumentException();
		}
		return x.compareTo(y);
	}

	public final static BigDecimal get(Heap heap, long addr) {
		int type = Unsafe.getByte(addr);
		if (type != Value.FORMAT_DECIMAL) {
			throw new IllegalArgumentException();
		}
		int scale = Unsafe.getByte(addr+1);
		BigInteger bigint = BigInt.get(heap, addr+2);
		BigDecimal bd = new BigDecimal(bigint, scale);
		return bd;
	}

	public final static int getSize(long pValue) {
		int size = BigInt.getSize(pValue + 2);
		size += 2;
		return size;
	}

	public static long abs(Heap heap, long pValue) {
		BigDecimal value = get(heap, pValue);
		if (value.signum() >= 0) {
			return pValue;
		}
		return FishNumber.allocSet(heap, value.abs());
	}

	public static long negate(Heap heap, long pValue) {
		BigDecimal value = get(heap, pValue);
		return FishNumber.allocSet(heap, value.negate());
	}
	
}
