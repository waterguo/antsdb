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
package com.antsdb.saltedfish.cpp;

import java.math.BigDecimal;
import java.math.BigInteger;

import com.antsdb.saltedfish.util.CodingError;

public class FishNumber extends Value {
	public final static boolean isNumber(long addr) {
		byte type = Unsafe.getByte(addr);
		byte kind = Value.getType(type);
		return (kind == Value.TYPE_NUMBER);
	}

	public final static long allocSet(Heap heap, BigDecimal result) {
		int scale = result.scale();
		BigInteger bigint = result.unscaledValue();
		byte[] bytes = bigint.toByteArray();
		long addr = heap.alloc(2 + 2 + bytes.length);
		Unsafe.putByte(addr, Value.FORMAT_DECIMAL);
		Unsafe.putByte(addr+1, (byte)scale);
		BigInt.set(heap, addr+2, bytes);
		return addr;
	}

	public static final long add(Heap heap, long addr1, long addr2) {
		int type1 = getFormat(heap, addr1);
		int type2 = getFormat(heap, addr2);
		if (type1 >= type2) {
			return add(heap, type1, addr1, type2, addr2);
		}
		else {
			return add(heap, type2, addr2, type1, addr1);
		}
	}
	
	public static final long multiply(Heap heap, long addr1, long addr2) {
		int type1 = getFormat(heap, addr1);
		int type2 = getFormat(heap, addr2);
		if (type1 >= type2) {
			return multiply(heap, type1, addr1, type2, addr2);
		}
		else {
			return multiply(heap, type2, addr2, type1, addr1);
		}
	}
	private static long multiply(Heap heap, int type2, long addr2, int type1, long addr1) {
		if (type1 == Value.FORMAT_INT4) {
			int value = Int4.get(heap, addr1);
			return multiply_int4(heap, value, type2, addr2);
		}
		else if (type1 == Value.FORMAT_INT8) {
			long value = Int8.get(heap, addr1);
			return multiply_int8(heap, value, type2, addr2);
		}
		else if (type1 == Value.FORMAT_FAST_DECIMAL) {
			BigDecimal value = FastDecimal.get(heap, addr1);
			return multiply_decimal(heap, value, type2, addr2);
		}
		else if (type1 == Value.FORMAT_DECIMAL) {
			BigDecimal value = FishDecimal.get(heap, addr1);
			return multiply_decimal(heap, value, type2, addr2);
		}
		else {
			throw new CodingError();
		}
	}

	private static long multiply_decimal(Heap heap, BigDecimal value, int type2, long addr2) {
		BigDecimal value2;
		if (type2 == Value.FORMAT_INT4) {
			value2 = BigDecimal.valueOf(Int4.get(heap, addr2));
		}
		else if (type2 == Value.FORMAT_INT8) {
			value2 = BigDecimal.valueOf(Int8.get(heap, addr2));
		}
		else if (type2 == Value.FORMAT_BIGINT) {
			value2 = new BigDecimal(BigInt.get(heap, addr2));
		}
		else if (type2 == Value.FORMAT_FAST_DECIMAL) {
			int scale2 = FastDecimal.getScale(heap, addr2);
			long unscaled2 = FastDecimal.getUnscaledLong(heap, addr2);
			value2 = BigDecimal.valueOf(unscaled2, scale2);
		}
		else if (type2 == Value.FORMAT_DECIMAL) {
			value2 = FishDecimal.get(heap, addr2);
		}
		else {
			throw new CodingError(); 
		}
		BigDecimal result = value.multiply(value2);
		return FishNumber.allocSet(heap, result);
	}

	private static long multiply_int8(Heap heap, long value1, int type2, long addr2) {
		long value2;
		if (type2 == Value.FORMAT_INT4) {
			value2 = Int4.get(heap, addr2);
		}
		else if (type2 == Value.FORMAT_INT8) {
			value2 = Int8.get(heap, addr2);
		}
		else {
			throw new CodingError(); 
		}
		long result = value1 * value2;
        if ( (((result ^ value1) & (result ^ value2))) >= 0L) {
        	// not overflow
        	long addr = Int8.allocSet(heap, result);
            return addr;
        }
        else {
        	// overflow
        	return multiply_bigint(heap, BigInteger.valueOf(value1), type2, addr2);
        }
	}

	private static long multiply_bigint(Heap heap, BigInteger value, int type2, long addr2) {
		BigInteger value2;
		if (type2 == Value.FORMAT_INT4) {
			value2 = BigInteger.valueOf(Int4.get(heap, addr2));
		}
		else if (type2 == Value.FORMAT_INT8) {
			value2 = BigInteger.valueOf(Int8.get(heap, addr2));
		}
		else if (type2 == Value.FORMAT_BIGINT) {
			value2 = BigInt.get(heap, addr2);
		}
		else {
			throw new CodingError(); 
		}
		BigInteger result = value.multiply(value2);
		return BigInt.allocSet(heap, result);
	}

	private static long multiply_int4(Heap heap, int value1, int type2, long addr2) {
		if (type2 == Value.FORMAT_INT4) {
			int value2 = Int4.get(heap, addr2);
			int result = value1 * value2;
	        if ( (((result ^ value1) & (result ^ value2))) >= 0L) {
	        	// not overflow
	        	long addr = Int4.allocSet(heap, result);
	            return addr;
	        }
	        else {
	        	// overflow
		        return multiply_int8(heap, value1, type2, addr2);
	        }
		}
		else {
			throw new CodingError();
		}
	}

	private static final long add(Heap heap, int type1, long addr1, int type2, long addr2) {
		if (type1 == Value.FORMAT_INT4) {
			int value = Int4.get(heap, addr1);
			return add_int4(heap, value, type2, addr2);
		}
		else if (type1 == Value.FORMAT_INT8) {
			long value = Int8.get(heap, addr1);
			return add_int8(heap, value, type2, addr2);
		}
		else if (type1 == Value.FORMAT_FAST_DECIMAL) {
			int scale = FastDecimal.getScale(heap, addr1);
			long unscaled = FastDecimal.getUnscaledLong(heap, addr1);
			return add_fast(heap, unscaled, scale, type2, addr2);
		}
		else if (type1 == Value.FORMAT_DECIMAL) {
			BigDecimal value = FishDecimal.get(heap, addr1);
			return add_decimal(heap, value, type2, addr2);
		}
		else {
			throw new CodingError();
		}
	}

	private static final long add_int4(Heap heap, int value1, int type2, long addr2) {
		if (type2 == Value.FORMAT_INT4) {
			int value2 = Int4.get(heap, addr2);
			int result = value1 + value2;
	        if ( (((result ^ value1) & (result ^ value2))) >= 0L) {
	        	// not overflow
	        	long addr = Int4.allocSet(heap, result);
	            return addr;
	        }
	        else {
	        	// overflow
		        return add_int8(heap, value1, type2, addr2);
	        }
		}
		else {
			throw new CodingError();
		}
	}

	private final static long add_int8(Heap heap, long value1, int type2, long addr2) {
		long value2;
		if (type2 == Value.FORMAT_INT4) {
			value2 = Int4.get(heap, addr2);
		}
		else if (type2 == Value.FORMAT_INT8) {
			value2 = Int8.get(heap, addr2);
		}
		else {
			throw new CodingError(); 
		}
		long result = value1 + value2;
        if ( (((result ^ value1) & (result ^ value2))) >= 0L) {
        	// not overflow
        	long addr = Int8.allocSet(heap, result);
            return addr;
        }
        else {
        	// overflow
        	return add_bigint(heap, BigInteger.valueOf(value1), type2, addr2);
        }
	}

	private final static long add_bigint(Heap heap, BigInteger value, int type2, long addr2) {
		BigInteger value2;
		if (type2 == Value.FORMAT_INT4) {
			value2 = BigInteger.valueOf(Int4.get(heap, addr2));
		}
		else if (type2 == Value.FORMAT_INT8) {
			value2 = BigInteger.valueOf(Int8.get(heap, addr2));
		}
		else if (type2 == Value.FORMAT_BIGINT) {
			value2 = BigInt.get(heap, addr2);
		}
		else {
			throw new CodingError(); 
		}
		BigInteger result = value.add(value2);
		return BigInt.allocSet(heap, result);
	}

	private final static long add_decimal(Heap heap, BigDecimal value, int type2, long addr2) {
		BigDecimal value2;
		if (type2 == Value.FORMAT_INT4) {
			value2 = BigDecimal.valueOf(Int4.get(heap, addr2));
		}
		else if (type2 == Value.FORMAT_INT8) {
			value2 = BigDecimal.valueOf(Int8.get(heap, addr2));
		}
		else if (type2 == Value.FORMAT_BIGINT) {
			value2 = new BigDecimal(BigInt.get(heap, addr2));
		}
		else if (type2 == Value.FORMAT_FAST_DECIMAL) {
			int scale2 = FastDecimal.getScale(heap, addr2);
			long unscaled2 = FastDecimal.getUnscaledLong(heap, addr2);
			value2 = BigDecimal.valueOf(unscaled2, scale2);
		}
		else if (type2 == Value.FORMAT_DECIMAL) {
			value2 = FishDecimal.get(heap, addr2);
		}
		else {
			throw new CodingError(); 
		}
		BigDecimal result = value.add(value2);
		return FishNumber.allocSet(heap, result);
	}

	private final static long add_fast(Heap heap, long unscaled1, int scale1, int type2, long addr2) {
		long unscaled2;
		int scale2;
		if (type2 == Value.FORMAT_INT4) {
			scale2 = 0;
			unscaled2 = Int4.get(heap, addr2);
		}
		else if (type2 == Value.FORMAT_INT8) {
			scale2 = 0;
			unscaled2 = Int8.get(heap, addr2);
		}
		else if (type2 == Value.FORMAT_BIGINT) {
			BigDecimal bd = BigDecimal.valueOf(unscaled1, scale1);
			return add_decimal(heap, bd, type2, addr2);
		}
		else if (type2 == Value.FORMAT_FAST_DECIMAL) {
			scale2 = FastDecimal.getScale(heap, addr2);
			unscaled2 = FastDecimal.getUnscaledLong(heap, addr2);
		}
		else {
			throw new CodingError(); 
		}
		long addr = FastDecimal.add(heap, unscaled1, scale1, unscaled2, scale2);
		if (addr != FastDecimal.INFLATED) {
			return addr;
		}
		else {
			// overflow
			BigDecimal bd = BigDecimal.valueOf(unscaled1, scale1);
			return add_decimal(heap, bd, type2, addr2);
		}
	}

	public final static int intValue(long addr) {
		byte type = Unsafe.getByte(addr);
		if (type == Value.FORMAT_INT4) {
			return Int4.get(addr);
		}
		else {
			throw new ArithmeticException();
		}
	}

	public final static long longValue(long addr) {
		byte type = Unsafe.getByte(addr);
		if (type == Value.FORMAT_INT4) {
			return Int4.get(addr);
		}
		else if (type == Value.FORMAT_INT8) {
			return Int8.get(null, addr);
		}
		else if (type == Value.FORMAT_DECIMAL) {
			BigDecimal dec = FishDecimal.get(null, addr);
			return dec.longValueExact();
		}
		else {
			throw new ArithmeticException();
		}
	}

	final static boolean equals(long addr1, long addr2) {
		return compare(addr1, addr2) == 0;
	}

	final static int compare(long addr1, long addr2) {
		byte type1 = Unsafe.getByte(addr1);
		byte type2 = Unsafe.getByte(addr2);
		if (type1 >= type2) {
			return compare(type1, addr1, type2, addr2);
		}
		else {
			return -compare(type2, addr2, type1, addr1);
		}
	}
	
	public final static int compare(byte type1, long addr1, byte type2, long addr2) {
		switch (type1) {
		case Value.FORMAT_DECIMAL:
			return FishDecimal.compare(addr1, type2, addr2);
		case Value.FORMAT_FAST_DECIMAL:
			return FastDecimal.compare(addr1, type2, addr2);
		case Value.FORMAT_INT8:
			return Int8.compare(addr1, type2, addr2);
		case Value.FORMAT_INT4:
			return Int4.compare(addr1, type2, addr2);
		default:
			throw new IllegalArgumentException();
		}
	}

	public static long abs(Heap heap, long pValue) {
		int format = Value.getFormat(null, pValue);
		switch (format) {
		case Value.FORMAT_DECIMAL:
			return FishDecimal.abs(heap, pValue);
		case Value.FORMAT_FAST_DECIMAL:
			return FastDecimal.abs(heap, pValue);
		case Value.FORMAT_INT8:
			return Int8.abs(heap, pValue);
		case Value.FORMAT_INT4:
			return Int4.abs(heap, pValue);
		default:
			throw new IllegalArgumentException();
		}
	}

	public static long negate(Heap heap, long pValue) {
		int format = Value.getFormat(null, pValue);
		switch (format) {
		case Value.FORMAT_DECIMAL:
			return FishDecimal.negate(heap, pValue);
		case Value.FORMAT_FAST_DECIMAL:
			return FastDecimal.negate(heap, pValue);
		case Value.FORMAT_INT8:
			return Int8.negate(heap, pValue);
		case Value.FORMAT_INT4:
			return Int4.negate(heap, pValue);
		default:
			throw new IllegalArgumentException();
		}
	}

}
