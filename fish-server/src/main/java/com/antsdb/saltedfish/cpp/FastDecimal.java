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

public final class FastDecimal extends BrutalMemoryObject {
    /**
     * Sentinel value for {@link #intCompact} indicating the
     * significand information is only available from {@code intVal}.
     */
    static final long INFLATED = Long.MIN_VALUE;
    
    private static final long[] LONG_TEN_POWERS_TABLE = {
            1,                     // 0 / 10^0
            10,                    // 1 / 10^1
            100,                   // 2 / 10^2
            1000,                  // 3 / 10^3
            10000,                 // 4 / 10^4
            100000,                // 5 / 10^5
            1000000,               // 6 / 10^6
            10000000,              // 7 / 10^7
            100000000,             // 8 / 10^8
            1000000000,            // 9 / 10^9
            10000000000L,          // 10 / 10^10
            100000000000L,         // 11 / 10^11
            1000000000000L,        // 12 / 10^12
            10000000000000L,       // 13 / 10^13
            100000000000000L,      // 14 / 10^14
            1000000000000000L,     // 15 / 10^15
            10000000000000000L,    // 16 / 10^16
            100000000000000000L,   // 17 / 10^17
            1000000000000000000L   // 18 / 10^18
        };
    
    private static final long THRESHOLDS_TABLE[] = {
            Long.MAX_VALUE,                     // 0
            Long.MAX_VALUE/10L,                 // 1
            Long.MAX_VALUE/100L,                // 2
            Long.MAX_VALUE/1000L,               // 3
            Long.MAX_VALUE/10000L,              // 4
            Long.MAX_VALUE/100000L,             // 5
            Long.MAX_VALUE/1000000L,            // 6
            Long.MAX_VALUE/10000000L,           // 7
            Long.MAX_VALUE/100000000L,          // 8
            Long.MAX_VALUE/1000000000L,         // 9
            Long.MAX_VALUE/10000000000L,        // 10
            Long.MAX_VALUE/100000000000L,       // 11
            Long.MAX_VALUE/1000000000000L,      // 12
            Long.MAX_VALUE/10000000000000L,     // 13
            Long.MAX_VALUE/100000000000000L,    // 14
            Long.MAX_VALUE/1000000000000000L,   // 15
            Long.MAX_VALUE/10000000000000000L,  // 16
            Long.MAX_VALUE/100000000000000000L, // 17
            Long.MAX_VALUE/1000000000000000000L // 18
        };
    
    static final BigInteger MAX_VALUE = BigInteger.valueOf(Long.MAX_VALUE);
    static final BigInteger MIN_VALUE = BigInteger.valueOf(Long.MIN_VALUE);
    
    public FastDecimal(long addr) {
        super(addr);
    }

    public final static BigDecimal get(Heap heap, long addr) {
        int type = Unsafe.getByte(addr);
        if (type != Value.FORMAT_FAST_DECIMAL) {
            throw new IllegalArgumentException();
        }
        int scale = Unsafe.getByte(addr+1);
        long unscaled = Unsafe.getLong(addr+2);
        BigDecimal value = BigDecimal.valueOf(unscaled, scale);
        return value;
    }

    public final static byte getScale(Heap heap, long addr) {
        byte scale = Unsafe.getByte(addr+1);
        return scale;
    }
    
    public final static long getUnscaledLong(Heap heap, long addr) {
        long unscaled = Unsafe.getLong(addr+2);
        return unscaled;
    }
    
    public final static long allocSet(Heap heap, BigDecimal value) {
        BigInteger unscaled = value.unscaledValue();
        int scale = value.scale();
        if ((scale > Byte.MAX_VALUE) || (scale < 0)) {
            throw new ArithmeticException();
        }
        if ((unscaled.compareTo(MAX_VALUE) > 0) || (unscaled.compareTo(MIN_VALUE) < 0)) {
            throw new ArithmeticException();
        }
        return allocSet(heap, unscaled.longValue(), (byte)scale);
    }

    public final static long allocSet(Heap heap, long value, byte scale) {
        long addr = heap.alloc(10);
        set(heap, addr, value, scale);
        return addr;
    }

    public final static void set(Heap heap, long addr, long value, byte scale) {
        Unsafe.putByte(addr, Value.FORMAT_FAST_DECIMAL);
        Unsafe.putByte(addr+1, scale);
        Unsafe.putLong(addr+2, value);
    }

    /*
     * returns INFLATED if oveflow
     */
    private static long add(long xs, long ys){
        long sum = xs + ys;
        // See "Hacker's Delight" section 2-12 for explanation of
        // the overflow test.
        if ( (((sum ^ xs) & (sum ^ ys))) >= 0L) { // not overflowed
            return sum;
        }
        return INFLATED;
    }
    
    private static long add(Heap heap, long xs, long ys, int scale){
        long sum = add(xs, ys);
        if (sum!=INFLATED)
            return allocSet(heap, sum, (byte)scale);
        return INFLATED;
    }

    private static int checkScale(long intCompact, long val) {
        int asInt = (int)val;
        if (asInt != val) {
            asInt = val>Byte.MAX_VALUE ? Byte.MAX_VALUE : Byte.MIN_VALUE;
            if (intCompact != 0)
                throw new ArithmeticException(asInt>0 ? "Underflow":"Overflow");
        }
        return asInt;
    }
    
    final static long add(Heap heap, final long value1, int scale1, final long value2, int scale2) {
        long sdiff = (long) scale1 - scale2;
        if (sdiff == 0) {
            return add(heap, value1, value2, scale1);
        } 
        else if (sdiff < 0) {
            int raise = checkScale(value1,-sdiff);
            long scaledX = longMultiplyPowerTen(value1, raise);
            if (scaledX != INFLATED) {
                return add(heap, scaledX, value2, scale2);
            } 
            else {
                return INFLATED;
            }
        } 
        else {
            int raise = checkScale(value2,sdiff);
            long scaledY = longMultiplyPowerTen(value2, raise);
            if (scaledY != INFLATED) {
                return add(heap, value1, scaledY, scale1);
            } 
            else {
                return INFLATED;
            }
        }
    }
    
    /**
     * Compute val * 10 ^ n; return this product if it is
     * representable as a long, INFLATED otherwise.
     */
    private static long longMultiplyPowerTen(long val, int n) {
        if (val == 0 || n <= 0)
            return val;
        long[] tab = LONG_TEN_POWERS_TABLE;
        long[] bounds = THRESHOLDS_TABLE;
        if (n < tab.length && n < bounds.length) {
            long tenpower = tab[n];
            if (val == 1)
                return tenpower;
            if (Math.abs(val) <= bounds[n])
                return val * tenpower;
        }
        return INFLATED;
    }

    final static int compare(long addr1, byte type2, long addr2) {
        long x_digits = Unsafe.getLong(addr1+2);
        int x_scale = Unsafe.getByte(addr1+1);
        long y_digits;
        int y_scale;
        switch (type2) {
        case Value.FORMAT_INT8:
            y_digits = Int8.get(null, addr2);
            y_scale = 0;
            break;
        case Value.FORMAT_INT4:
            y_digits = Int4.get(addr2);
            y_scale = 0;
            break;
        default:
            throw new IllegalArgumentException();
        }
        if (x_scale > y_scale) {
            long factor = 10 ^ (x_scale - y_scale);
            long result = x_digits * factor;
            if (result / factor == x_digits) {
                x_digits = result;
            }
            else {
                return compare_overflow(x_digits, x_scale, y_digits, y_scale);
            }
        }
        else if (x_scale < y_scale) {
            long factor = 10 ^ (y_scale - x_scale);
            long result = y_digits * factor;
            if (result / factor == y_digits) {
                y_digits = result;
            }
            else {
                return compare_overflow(x_digits, x_scale, y_digits, y_scale);
            }
        }
        return x_digits != y_digits ? ((x_digits > y_digits) ? 1 : -1) : 0;
    }

    private static int compare_overflow(long x_digits, int x_scale, long y_digits, int y_scale) {
        long x_signum = (x_digits >> 63) & 1;
        long y_signum = (y_digits >> 63) & 1;
        if (x_signum != y_signum) {
            return x_signum > y_signum ? 1 : -1;
        }
        BigDecimal x = BigDecimal.valueOf(x_digits, x_scale);
        BigDecimal y = BigDecimal.valueOf(y_digits, y_scale);
        return x.compareTo(y);
    }

    public static long abs(Heap heap, long pValue) {
        long unscaled = getUnscaledLong(heap, pValue);
        if (unscaled >= 0) {
            return pValue;
        }
        return allocSet(heap, -unscaled, getScale(heap, pValue));
    }

    public static long negate(Heap heap, long pValue) {
        long unscaled = getUnscaledLong(heap, pValue);
        return allocSet(heap, -unscaled, getScale(heap, pValue));
    }

    @Override
    public int getByteSize() {
        return 10;
    }

    @Override
    public int getFormat() {
        return Value.FORMAT_FAST_DECIMAL;
    }
}
