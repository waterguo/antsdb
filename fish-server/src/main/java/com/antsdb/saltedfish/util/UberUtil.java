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
package com.antsdb.saltedfish.util;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sun.management.OperatingSystemMXBean;

import sun.misc.Unsafe;

import org.apache.commons.io.HexDump;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * Created by wguo on 15-01-06.
 */
@SuppressWarnings("restriction")
public class UberUtil {

    private static final Unsafe unsafe;
    private static Field ADDRESS_FIELD;

    static {
        try {
            ADDRESS_FIELD = Buffer.class.getDeclaredField("address");
            ADDRESS_FIELD.setAccessible(true);
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Unsafe getUnsafe() {
        return unsafe;
    }

    public static String getThisClassName() {
        String result = "";
        Exception x = new Exception();
        StackTraceElement[] stack = x.getStackTrace();
        if (stack != null) {
            if (stack.length >= 2) {
                result = stack[1].getClassName();
                if ("com.antsdb.saltedfish.util.UberUtil$getThisClassName".equals(result)) {
                    // this is groovy
                    if (stack.length > 5) {
                        result = stack[5].getClassName();
                    }
                }
            }
        }
        return result;
    }

    public static Class<?> getThisClass() {
        try {
            String className = "";
            Exception x = new Exception();
            StackTraceElement[] stack = x.getStackTrace();
            if (stack != null) {
                if (stack.length >= 2) {
                    className = stack[1].getClassName();
                }
            }
            return Class.forName(className);
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static Logger getThisLogger() {
        Exception x = new Exception();
        StackTraceElement[] stack = x.getStackTrace();
        String className = "";
        if (stack != null) {
            if (stack.length >= 2) {
                className = stack[1].getClassName();
            }
        }
        return LoggerFactory.getLogger(className);
    }

    public static boolean safeEqual(Object obj1, Object obj2) {
        if (obj1 == obj2) {
            return true;
        }
        if ((obj1 == null) || (obj2 == null)) {
            return false;
        }
        return obj1.equals(obj2);
    }

    public static int safeCompare(Object val1, Object val2) {
        if ((val1 == null) && (val2 == null)) {
            return 0;
        }
        if ((val1 == null) && (val2 != null)) {
            return -1;
        }
        if ((val1 != null) && (val2 == null)) {
            return 1;
        }
        if (val1.getClass() != val2.getClass()) {
            if ((val1 instanceof Number) && (val2 instanceof Number)) {
                double dx = ((Number)val1).doubleValue();
                double dy = ((Number)val2).doubleValue();
                return Double.compare(dx, dy);
            }
            throw new IllegalArgumentException(val1.getClass().getName() + " " + val2.getClass().getName());
        }
        if (val1 instanceof String) {
            return ((String) val1).compareTo((String) val2);
        }
        else if (val1 instanceof Integer) {
            return ((Integer) val1).compareTo((Integer) val2);
        }
        else if (val1 instanceof Long) {
            return ((Long) val1).compareTo((Long) val2);
        }
        else if (val1 instanceof Float) {
            return ((Float) val1).compareTo((Float) val2);
        }
        else if (val1 instanceof Double) {
            return ((Double) val1).compareTo((Double) val2);
        }
        else if (val1 instanceof BigDecimal) {
            return ((BigDecimal) val1).compareTo((BigDecimal) val2);
        }
        else if (val1 instanceof Date) {
            return ((Date) val1).compareTo((Date) val2);
        }
        else if (val1 instanceof Timestamp) {
            return ((Timestamp) val1).compareTo((Timestamp) val2);
        }
        else if (val1 instanceof byte[]) {
            return compare((byte[]) val1, (byte[]) val2);
        }
        else if (val1 instanceof byte[]) {
            return compare((byte[]) val1, (byte[])val2);
        }
        throw new IllegalArgumentException("type: " + val1.getClass());
    }

    private static int compare(byte[] x, byte[] y) {
        int minLenght = Math.min(x.length,  y.length);
        for (int i=0; i<minLenght; i++) {
            int result = Integer.compare(x[i] & 0xff, y[i] & 0xff);
            if (result != 0) {
                return result;
            }
        }
        if (x.length > minLenght) {
            return 1;
        }
        else if (y.length > minLenght) {
            return -1;
        }
        else {
            return 0;
        }
    }
    
    @SuppressWarnings("unchecked")
    public static <T> T toObject(Class<T> klass, Object val) {
        if (val == null) {
            return null;
        }
        if (klass.isInstance(val)) {
            return (T) val;
        }
        if (val instanceof byte[]) {
            if (((byte[]) val).length == 0) {
                return null;
            }
            val = new String((byte[]) val, Charsets.UTF_8);
        }
        if (klass == String.class) {
            return (T) String.valueOf(val);
        }
        if (val instanceof String) {
            String text = (String) val;
            if (klass == String.class) {
                return (T) text;
            }
            if (text.length() == 0) {
                return null;
            }
            if (klass == Integer.class) {
                return (T) new Integer(text);
            }
            else if (klass == Long.class) {
                return (T) new Long(text);
            }
            else if (klass == BigDecimal.class) {
                return (T) new BigDecimal(text);
            }
            else if (klass == Timestamp.class) {
                return (T) Timestamp.valueOf(text);
            }
            else if (klass == Date.class) {
                return (T) Date.valueOf(text);
            }
            else if (klass == Boolean.class) {
                return (T) new Boolean(text);
            }
            else if (klass == Double.class) {
                return (T) new Double(text);
            }
        }
        if (val instanceof BigDecimal) {
            if (klass == Long.class) {
                Long n = ((BigDecimal) val).longValueExact();
                return (T) n;
            }
            else if (klass == Integer.class) {
                Integer n = ((BigDecimal) val).intValueExact();
                return (T) n;
            }
            else if (klass == Double.class) {
                Double n = ((BigDecimal) val).doubleValue();
                return (T) n;
            }
            else if (klass == Boolean.class) {
                Integer n = ((BigDecimal) val).intValueExact();
                return (T) (Boolean) (n != 0);
            }
        }
        if (val instanceof Integer) {
            if (klass == BigDecimal.class) {
                return (T) BigDecimal.valueOf((Integer) val);
            }
            else if (klass == Long.class) {
                return (T) Long.valueOf((Integer) val);
            }
            else if (klass == Boolean.class) {
                Integer n = (Integer) val;
                return (T) (Boolean) (n != 0);
            }
        }
        if (val instanceof Long) {
            if (klass == BigDecimal.class) {
                return (T) BigDecimal.valueOf((Long) val);
            }
            else if (klass == Boolean.class) {
                Long n = (Long) val;
                return (T) (Boolean) (n != 0);
            }
        }
        if (val instanceof Boolean) {
            if (klass == Long.class) {
                return (T) Long.valueOf((Boolean) val ? 1 : 0);
            }
        }
        throw new IllegalArgumentException("class: " + val.getClass());
    }

    public static <T> Iterable<T> once(final Iterator<T> source) {
        return new Iterable<T>() {
            private AtomicBoolean exhausted = new AtomicBoolean();

            public Iterator<T> iterator() {
                Preconditions.checkState(!exhausted.getAndSet(true));
                return source;
            }
        };
    }

    /**
     * warning, this method has no consideration of performance
     * 
     * @param obj
     * @return
     */
    public static String toJson(Object obj) {
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(obj);
    }

    /**
     * warning, this method has no consideration of performance. it is written
     * to help logging objects
     * 
     * @param obj
     * @return
     */
    public static String toString(Object obj) {
        if (obj == null) {
            return "NULL";
        }
        StringBuilder buf = new StringBuilder();
        for (Field i : obj.getClass().getFields()) {
            if ((i.getModifiers() & Modifier.STATIC) != 0) {
                continue;
            }
            buf.append(i.getName());
            buf.append(":");
            Object value;
            try {
                value = i.get(obj);
            }
            catch (Exception x) {
                value = x.getMessage();
            }
            if (value != null) {
                buf.append(value.toString());
            }
            else {
                buf.append("NULL");
            }
            buf.append('\n');
        }
        return buf.toString();
    }

    public static String hexDump(byte[] bytes) {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        try {
            HexDump.dump(bytes, 0, buf, 0);
            return buf.toString();
        }
        catch (Exception x) {
        }
        return "";
    }

    public static byte[] toUtf8(String s) {
        return Charsets.UTF_8.encode(s).array();
    }

    public static long getAddress(ByteBuffer buf) {
        try {
            long address;
            address = ADDRESS_FIELD.getLong(buf);
            return address;
        }
        catch (Exception x) {
            throw new RuntimeException(x);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T clone(final T obj) throws CloneNotSupportedException {
        if (obj == null) {
            return null;
        }
        if (obj instanceof Cloneable) {
            Class<?> clazz = obj.getClass();
            Method m;
            try {
                m = clazz.getMethod("clone", (Class[]) null);
            }
            catch (NoSuchMethodException ex) {
                throw new NoSuchMethodError(ex.getMessage());
            }
            try {
                return (T) m.invoke(obj, (Object[]) null);
            }
            catch (InvocationTargetException ex) {
                Throwable cause = ex.getCause();
                if (cause instanceof CloneNotSupportedException) {
                    throw ((CloneNotSupportedException) cause);
                }
                else {
                    throw new Error("Unexpected exception", cause);
                }
            }
            catch (IllegalAccessException ex) {
                throw new IllegalAccessError(ex.getMessage());
            }
        }
        else {
            throw new CloneNotSupportedException();
        }
    }

    public static <T> List<T> runParallel(int nThreads, Callable<T> callback) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(nThreads);
        List<Future<T>> futures = new ArrayList<>();
        for (int i = 0; i < nThreads; i++) {
            Callable<T> run = (Callable<T>) UberUtil.clone(callback);
            futures.add(pool.submit(run));
        }
        List<T> result = new ArrayList<>();
        for (Future<T> i : futures) {
            result.add(i.get());
        }
        pool.shutdown();
        return result;
    }

    public static double getSystemCpuLoad() {
        OperatingSystemMXBean bean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        return bean.getSystemCpuLoad();
    }

    public static double getProcessCpuLoad() {
        OperatingSystemMXBean bean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        return bean.getProcessCpuLoad();
    }

    public static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    
    public static <T,R> R ifNotNull(T x, Function<T, R> function) {
        return (x != null) ? function.apply(x) : null;
    }
    
    public static long throughput(long start, long end, long count) {
        long elapse = end - start;
        return elapse == 0 ? 0 : count * 1000 / elapse;
    }
    
    public static boolean between(long value, long min, long max) {
        return (value >= min) && (value <= max);
    }
}
