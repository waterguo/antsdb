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

package com.antsdb.mysql.network;

import java.nio.CharBuffer;

import com.antsdb.saltedfish.cpp.Unsafe;

/**
 * 
 * @author wgu0
 */
public class PacketUtil {
    public static final long NULL_LENGTH = -1;
    
    public static long skipStringWithLength(long addr) {
        int length = (int) readLength(addr);
        addr = skipLength(addr);
        return addr + length;
    }
    
    public static String readStringWithLength(long addr) {
        int length = (int) readLength(addr);
        if (length <= 0) {
            return null;
        }
        addr = skipLength(addr);
        StringBuilder buf = new StringBuilder();
        for (int i=0; i<length; i++) {
            int ch = Unsafe.getByte(addr + i) & 0xff;
            buf.append((char)ch);
        }
        return buf.toString();
    }

    public static long skipLength(long addr) {
        int length = Unsafe.getByte(addr) & 0xff;
        switch (length) {
        case 251:
            return addr+2;
        case 252:
            return addr+3;
        case 253:
            return addr+4;
        case 254:
            return addr+5;
        default:
            return addr+1;
        }
    }
    
    public static long readLength(long addr) {
        int length = Unsafe.getByte(addr) & 0xff;
        addr++;
        switch (length) {
        case 251:
            return NULL_LENGTH;
        case 252:
            return readShort(addr) & 0xffff;
        case 253:
            return readLongInt(addr) & 0xffffff;
        case 254:
            return readInt(addr) & 0xffffffff;
        default:
            return length;
        }
    }

    public static int readInt(long addr) {
        return Unsafe.getInt(addr);
    }

    public static int readLongInt(long addr) {
        return Unsafe.getInt3(addr);
    }

    public static long readLongLong(long addr) {
        return Unsafe.getLong(addr);
    }

    /**
     * read null terminated string
     * 
     * @param addr
     * @param maxLength
     * @return
     */
    public static String readString(long addr, int maxLength) {
        StringBuilder buf = new StringBuilder();
        for (int i=0; i<maxLength; i++) {
            int ch = Unsafe.getByte(addr + i);
            if (ch == 0) {
                break;
            }
            buf.append((char)ch);
        }
        return buf.toString();
    }

    public static CharBuffer readStringAsCharBufWithMysqlExtension(long addr, int maxLength) {
        if (maxLength == 0) {
            return null;
        }
        CharBuffer result = CharBuffer.allocate(maxLength);
        long pEnd = addr + maxLength;
        for (long p=addr; p<pEnd;) {
            long pNext = readMysqlExtension(result, p, pEnd);
            if (pNext != p) {
                p = pNext;
                continue;
            }
            int ch = Unsafe.getByte(p++);
            result.put((char)ch);
        }
        return result;
    }
    
    private static long readMysqlExtension(CharBuffer buf, long p, long pEnd) {
        // read the mysql extension prefix
        
        long pNext = skip(p, pEnd, "/*!");
        if (pNext == p) {
            return p;
        }
        p = pNext;
        
        // skip the version 
        
        p = skipUntil(p, pEnd, ' ');
        
        // read the real stuff
        
        while (p < pEnd) {
            pNext = skip(p, pEnd, "*/");
            if (p != pNext) {
                p = pNext;
                break;
            }
            char ch = (char)Unsafe.getByte(p++);
            buf.put(ch);
        }
        return p;
    }

    private static long skipUntil(long p, long pEnd, char ch) {
        while (p < pEnd && Unsafe.getByte(p) != ch) {
            p++;
        }
        return p;
    }

    private static long skip(long p, long pEnd, String string) {
        if (pEnd - p < string.length()) {
            return p;
        }
        for (int i=0; i<string.length(); i++) {
            if (string.charAt(i) != Unsafe.getByte(p + i)) {
                return p;
            }
        }
        return p + string.length();
    }

    public static CharBuffer readStringAsCharBuf(long addr, int maxLength) {
        if (maxLength == 0) {
            return null;
        }
        CharBuffer result = CharBuffer.allocate(maxLength);
        for (int i=0; i<maxLength; i++) {
            int ch = Unsafe.getByte(addr + i);
            if (ch == 0) {
                break;
            }
            result.put((char)ch);
        }
        return result;
    }

    public static String readFxiedLengthString(long addr) {
        byte[] bytes = new byte[8];
        Unsafe.getBytes(addr, bytes);
        return new String(bytes);
    }

    public static short readShort(long addr) {
        return Unsafe.getShort(addr);
    }

    public static byte readByte(long addr) {
        return Unsafe.getByte(addr);
    }

}
