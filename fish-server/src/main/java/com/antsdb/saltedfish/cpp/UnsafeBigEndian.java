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

/**
 * big endian system
 *  
 * @author wgu0
 */
public final class UnsafeBigEndian {
    private static byte long7(long x) { return (byte)(x >> 56); }
    private static byte long6(long x) { return (byte)(x >> 48); }
    private static byte long5(long x) { return (byte)(x >> 40); }
    private static byte long4(long x) { return (byte)(x >> 32); }
    private static byte long3(long x) { return (byte)(x >> 24); }
    private static byte long2(long x) { return (byte)(x >> 16); }
    private static byte long1(long x) { return (byte)(x >>  8); }
    private static byte long0(long x) { return (byte)(x      ); }
    private static byte int3(long x) { return (byte)(x >> 24); }
    private static byte int2(long x) { return (byte)(x >> 16); }
    private static byte int1(long x) { return (byte)(x >> 8); }
    private static byte int0(long x) { return (byte)(x     ); }
    
	public static void putLong(long addr, long value) {
		Unsafe.putByte(addr, long7(value));
		Unsafe.putByte(addr+1, long6(value));
		Unsafe.putByte(addr+2, long5(value));
		Unsafe.putByte(addr+3, long4(value));
		Unsafe.putByte(addr+4, long3(value));
		Unsafe.putByte(addr+5, long2(value));
		Unsafe.putByte(addr+6, long1(value));
		Unsafe.putByte(addr+7, long0(value));
	}
	
	public static void putInt(long addr, int value) {
		Unsafe.putByte(addr,   int3(value));
		Unsafe.putByte(addr+1, int2(value));
		Unsafe.putByte(addr+2, int1(value));
		Unsafe.putByte(addr+3, int0(value));
	}
	
	public static void copyMemory(long source, long target, long bytes) {
		Unsafe.copyMemory(source, target, bytes);
	}
	
	public static void putShort(long addr, short value) {
		Unsafe.putByte(addr, (byte)(value >> 8));
		Unsafe.putByte(addr+1, (byte)(value));
	}
	
	public static void putFloat(long addr, float value) {
		int bits = Float.floatToRawIntBits(value);
		putInt(addr, bits);
	}
	
	public static void putDouble(long addr, double value) {
		long bits = Double.doubleToLongBits(value);
		putLong(addr, bits);
	}
	
	public static void putByte(long pTarget, byte value) {
		Unsafe.putByte(pTarget, value);
	}
	
	public static void putInt3(long addr, int value) {
		UnsafeBigEndian.putShort(addr, (short)(value >> 8));
		UnsafeBigEndian.putByte(addr+2, (byte)value);
	}
	
	public static void putInt5(long addr, long value) {
		UnsafeBigEndian.putInt(addr, (int)(value >> 8));
		UnsafeBigEndian.putByte(addr + 4, (byte)value);
	}
	
	public static void putInt6(long addr, long value) {
		UnsafeBigEndian.putInt(addr, (int)(value >> 16));
		UnsafeBigEndian.putShort(addr+4, (short)value);
	}
	
	public static void putInt7(long addr, long n) {
		UnsafeBigEndian.putInt(addr, (int)(n >> 24));
		UnsafeBigEndian.putShort(addr+4, (short)(n >> 8));
		UnsafeBigEndian.putByte(addr+6, (byte)n);
	}
}
