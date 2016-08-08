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
package com.antsdb.saltedfish.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

import com.antsdb.saltedfish.sql.OrcaException;

/**
 * Created by wguo on 15-01-10.
 */
public class TypeSafeUtil {
    @SuppressWarnings("unchecked")
    public static <T> T upcast(Class<T> klass, Object val) {
        if (val == null) {
            return null;
        }
        if (val.getClass() == klass) {
            return (T)val;
        }
        if (klass == BigDecimal.class) {
            if (val instanceof Integer) {
                return (T)new BigDecimal((Integer)val);
            }
            else if (val instanceof Long) {
                return (T)new BigDecimal((Long)val);
            }
            else {
                throw new IllegalArgumentException("cannot upcast: " + val.getClass());
            }
        }
        else if (klass == Long.class) {
            if (val instanceof Integer) {
                return (T)Long.valueOf((Integer)val);
            }
            if (val instanceof BigDecimal) {
                return (T)(Long)((BigDecimal)val).longValueExact();
            }
        }
        else if (klass == Integer.class) {
            if (val instanceof BigDecimal) {
                return (T)(Integer)((BigDecimal)val).intValueExact();
            }
        }
        throw new IllegalArgumentException("cannot upcast: " + val.getClass());
    }

    public static Class<?> getUpperClass(Object... objs) {
        Class<?> ret = null;
        for (Object i:objs) {
            if (i == null) continue;
            if (ret == null) {
                ret = i.getClass();
                continue;
            }
            if (i.getClass() == ret) {
                continue;
            }
        }
        return ret;
    }

    @SuppressWarnings("unchecked")
    public static int compare(Object val1, Object val2) {
        if ((val1 == null) && (val2 == null)) {
            return 0;
        }
        if ((val1 == null) && (val2 != null)) {
            return -1;
        }
        if ((val1 != null) && (val2 == null)) {
            return 1;
        }
        if ((val1.getClass() == val2.getClass()) && (val1 instanceof Comparable)) {
            Comparable<Object> cval1 = (Comparable<Object>)val1;
            Comparable<Object> cval2 = (Comparable<Object>)val2;
            return cval1.compareTo(cval2);
        }
        else if (isNumber(val1) && isNumber(val2)) {
            BigDecimal number1 = toBigDecimal(val1);
            BigDecimal number2 = toBigDecimal(val2);
            return number1.compareTo(number2);
        }
        else if ((val1 instanceof Date) && (val2 instanceof Date)) {
            long time1 = ((Date)val1).getTime();
            long time2 = ((Date)val2).getTime();
            return (int)(time1-time2);
        }
        else if ((val1 instanceof String) || (val2 instanceof String)) {
            val1 = toString(val1);
            val2 = toString(val2);
            return compare(val1, val2);
        }
        else if ((val1 instanceof byte[]) || (val2 instanceof byte[])) {
        	byte[] array1 = toByteArray(val1);
        	byte[] array2 = toByteArray(val2);
        	for (int i=0; i<Math.min(array1.length, array2.length); i++) {
        		int comp = Byte.compare(array1[i], array2[2]);
        		if (comp != 0) {
        			return comp;
        		}
        	}
        	if (array1.length > array2.length) {
        		return 1;
        	}
        	else if (array1.length < array2.length) {
        		return -1;
        	}
        	return 0;
        }
        throw new IllegalArgumentException();
    }

    private static byte[] toByteArray(Object value) {
    	if (value == null) {
    		return null;
    	}
    	else if (value instanceof byte[]) {
    		return (byte[])value;
    	}
    	else {
    		throw new IllegalArgumentException();
    	}
	}

	private static String toString(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof byte[]) {
            return new String((byte[])value);
        }
        if (value instanceof String) {
            return (String)value;
        }
        return value.toString();
    }

    public static boolean equal(Object val1, Object val2) {
        return compare(val1, val2) == 0;
    }

    static boolean isNumber(Object val) {
        if (val instanceof Number) {
            return true;
        }
        if (val instanceof Boolean) {
            return true;
        }
        return false;
    }

    public static BigDecimal toBigDecimal(Object val) {
        if (val == null) {
            return null;
        }
        if (val instanceof Float) {
            return new BigDecimal((Float)val);
        }
        if (val instanceof Double) {
            return new BigDecimal((Double)val);
        }
        if (val instanceof BigInteger) {
            return new BigDecimal((BigInteger)val);
        }
        if (val instanceof Boolean) {
            return new BigDecimal(((Boolean)val) ? 1 : 0);
        }
        if (val instanceof Number) {
            return new BigDecimal(((Number)val).longValue());
        }
        throw new IllegalArgumentException();
    }
    
    /**
     * upcast the value left to the high class of the both
     * 
     * @param left
     * @param right
     * @return
     */
    public static Object upcast(Object left, Object right) {
        Object casted = left;
        if ((left == null) || (right == null)) {
            casted = left;
        }
        else if (left.getClass() == right.getClass()) {
            casted = left;
        }
        else if (right.getClass() == String.class) {
            casted = left.toString();
        }
        else if (right.getClass() == BigDecimal.class) {
            if (left.getClass() == BigInteger.class) {
                casted = new BigDecimal((BigInteger)left); 
            }
            else if (left instanceof Number) {
                casted = BigDecimal.valueOf(((Number)left).longValue());
            }
        }
        else if (right.getClass() == BigInteger.class) {
            if (left.getClass() == Long.class) {
                casted = BigInteger.valueOf((Long)left); 
            }
            else if (left.getClass() == Integer.class) {
                casted = BigInteger.valueOf((Integer)left); 
            }
        }
        else if (right.getClass() == Long.class) {
            if (left.getClass() == Integer.class) {
                casted = Long.valueOf((Integer)left); 
            }
        }
        return casted;
    }

    public static Object castAndDo(
            Object leftValue, 
            Object rightValue, 
            TernaryFunction<Class<?>, Object, Object, Object> func) {
        Object leftValueCasted = upcast(leftValue, rightValue);
        Object rightValueCasted = upcast(rightValue, leftValue);
        if ((leftValueCasted != null) && (rightValueCasted != null)) {
            if (leftValueCasted.getClass() != rightValueCasted.getClass()) {
                String classLeft = leftValueCasted.getClass().getSimpleName();
                String classRight = rightValueCasted.getClass().getSimpleName();
                throw new OrcaException("unable to cast value between " + classLeft + " and " + classRight);
            }
        }
        Class<?> type = null;
        if (leftValueCasted != null) {
            type = leftValueCasted.getClass();
        }
        else if (rightValueCasted != null) {
            type = rightValueCasted.getClass();
        }
        Object result = func.apply(type, leftValueCasted, rightValueCasted);
        return result;
    }
}
