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

import java.nio.charset.Charset;

import com.antsdb.saltedfish.cpp.Unsafe;

public class BytesUtil {
    static Charset UTF8 = Charset.forName("utf-8");
	private static final char[] _hexcodes = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E',
			'F' };
	private static final int[] _shifts = { 4, 0 };

    public static byte[] toUtf8(String s) {
        return s.getBytes(UTF8);
    }
    
    public static String toHex(byte value) {
        StringBuilder buf = new StringBuilder();
        dump(buf, value);
        return buf.toString();
    }

    public static String toHex(char value) {
        StringBuilder buf = new StringBuilder();
        dump(buf, (byte)(value >>> 8));
        dump(buf, (byte)(value & 0xff));
        return buf.toString();
    }

    public static String toHex(int value) {
        	StringBuilder buf = new StringBuilder();
        dump(buf, (byte)(value >>> 24));
        dump(buf, (byte)(value >>> 16 & 0xff));
        dump(buf, (byte)(value >>> 8 & 0xff));
        dump(buf, (byte)(value & 0xff));
        	return buf.toString();
    }
    
    public static String toHex8(byte[] bytes) {
        	if (bytes == null) {
        		return "";
        	}
        	StringBuilder buf = new StringBuilder();
        	for (int i=0; i<bytes.length; i++) {
        		if ((i > 0) && (i % 8 == 0)) {
        			buf.append('-');
        		}
        		dump(buf, bytes[i]);
        	}
        	return buf.toString();
    }
    
    public static String toHex(byte[] bytes) {
        	if (bytes == null) {
        		return "";
        	}
        	StringBuilder buf = new StringBuilder();
        	for (int i=0; i<bytes.length; i++) {
        		if ((i > 0) && (i % 16 == 0)) {
        			buf.append('\n');
        		}
        		dump(buf, bytes[i]);
        		buf.append(' ');
        	}
        	return buf.toString();
    }

    public static String toCompactHex(byte[] bytes) {
        	if (bytes == null) {
        		return "";
        	}
        	StringBuilder buf = new StringBuilder();
        	for (int i=0; i<bytes.length; i++) {
        		dump(buf, bytes[i]);
        	}
        	return buf.toString();
    }
    
    public static String toCompactHex(long p, int len) {
        	if (p == 0) {
        		return "";
        	}
        	StringBuilder buf = new StringBuilder();
        	for (int i=0; i<len; i++) {
        		buf.append(toHex(Unsafe.getByte(p + i)));
        	}
        	return buf.toString();
    }
    
    public static String toHex8(long p, int len) {
        	byte[] bytes = new byte[len];
        	for (int i=0; i<len; i++) {
        		bytes[i] = Unsafe.getByte(p + i);
        	}
        	return toHex8(bytes);
    }
    
    public static String toHex(long p, int len) {
        	byte[] bytes = new byte[len];
        	for (int i=0; i<len; i++) {
        		bytes[i] = Unsafe.getByte(p + i);
        	}
        	return toHex(bytes);
    }
    
	private static void dump(StringBuilder buf, byte value) {
		for (int j = 0; j < 2; j++) {
			int idx = (value >> _shifts[j]) & 15;
			buf.append(_hexcodes[idx]);
		}
	}

	public static byte[] hexToBytes(String value) {
		if (value.length() % 2 != 0) {
			throw new IllegalArgumentException();
		}
		byte[] bytes = new byte[value.length() / 2];
		for (int i=0; i<value.length(); i+=2) {
			bytes[i/2] = (byte)((Character.digit(value.charAt(i), 16) << 4) + Character.digit(value.charAt(i+1), 16));
		}
		return bytes;
	}
}
