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
package com.antsdb.saltedfish.server.mysql.util;

import io.netty.buffer.ByteBuf;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.Calendar;

import org.slf4j.Logger;

import com.antsdb.saltedfish.charset.Decoder;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * this class is mapped to com.mysql.jdbc.Buffer from MySQL Connector 
 * 
 * @author xinyi
 *
 */
public final class BufferUtils {
    static Logger _log = UberUtil.getThisLogger();

    public static final long NULL_LENGTH = -1;
    private static final byte[] EMPTY_BYTES = new byte[0];

    public static int readLongInt(ByteBuf buf) {
        byte b0 = buf.readByte();
        byte b1 = buf.readByte();
        byte b2 = buf.readByte();
        int n = (b0 & 0xff) | ((b1 & 0xff) << 8) | ((b2 & 0xff) << 16);
        return n;
    }

    public static int readLongInt(ByteBuffer buf) {
        byte b0 = buf.get();
        byte b1 = buf.get();
        byte b2 = buf.get();
        int n = (b0 & 0xff) | ((b1 & 0xff) << 8) | ((b2 & 0xff) << 16);
        return n;
    }

    public static String readString(ByteBuf buf) {
        String s;
        int bytes = buf.bytesBefore((byte)0);
        if (bytes == -1) {
            // string might not be terminated by 0
            bytes = buf.readableBytes();
            s = buf.toString(buf.readerIndex(), bytes, Charset.defaultCharset());
            buf.skipBytes(bytes);
        }
        else {
            s = buf.toString(buf.readerIndex(), bytes, Charset.defaultCharset());
            buf.skipBytes(bytes+1);
        }
        return s;
    }

    public static String readString(ByteBuffer buf) {
        String result;
        int bytes = -1;
        for (int i=buf.position(); i<=buf.limit(); i++) {
            if (buf.get(i) == 0) {
                bytes = i - buf.position();
                break;
            }
        }
        if (bytes == -1) {
            // string might not be terminated by 0
            result = Charset.defaultCharset().decode(buf).toString();
        }
        else {
            int keep = buf.limit();
            buf.limit(buf.position() + bytes);
            result = Charset.defaultCharset().decode(buf).toString();
            buf.limit(keep);
            buf.get();
        }
        return result;
    }

    public static int readLong(ByteBuf in) {
        int l = (int) (in.readByte() & 0xff);
        l |= (int) (in.readByte() & 0xff) << 8;
        l |= (int) (in.readByte() & 0xff) << 16;
        l |= (int) (in.readByte() & 0xff) << 24;
        return l;
    }

    public static int readLong(ByteBuffer in) {
        int l = (int) (in.get() & 0xff);
        l |= (int) (in.get() & 0xff) << 8;
        l |= (int) (in.get() & 0xff) << 16;
        l |= (int) (in.get() & 0xff) << 24;
        return l;
    }

    public static long readLongLong(ByteBuf in) {
        long l = (long) (in.readByte() & 0xff);
        l |= (long) (in.readByte() & 0xff) << 8;
        l |= (long) (in.readByte() & 0xff) << 16;
        l |= (long) (in.readByte() & 0xff) << 24;
        l |= (long) (in.readByte() & 0xff) << 32;
        l |= (long) (in.readByte() & 0xff) << 40;
        l |= (long) (in.readByte() & 0xff) << 48;
        l |= (long) (in.readByte() & 0xff) << 56;
        return l;
    }

    public static long readLongLong(ByteBuffer in) {
        long l = (long) (in.get() & 0xff);
        l |= (long) (in.get() & 0xff) << 8;
        l |= (long) (in.get() & 0xff) << 16;
        l |= (long) (in.get() & 0xff) << 24;
        l |= (long) (in.get() & 0xff) << 32;
        l |= (long) (in.get() & 0xff) << 40;
        l |= (long) (in.get() & 0xff) << 48;
        l |= (long) (in.get() & 0xff) << 56;
        return l;
    }

    public static double readDouble(ByteBuf in) {
        return Double.longBitsToDouble(readLongLong(in));
    }

    public static double readDouble(ByteBuffer in) {
        return Double.longBitsToDouble(readLongLong(in));
    }

    public static int readInt(ByteBuf in) {
        byte b0 = in.readByte();
        byte b1 = in.readByte();
        return (b0 & 0xff) | ((b1 & 0xff) << 8);
    }

    public static int readInt(ByteBuffer in) {
        byte b0 = in.get();
        byte b1 = in.get();
        return (b0 & 0xff) | ((b1 & 0xff) << 8);
    }

    public static String readStringFixLength(ByteBuf in, int length) {
        if (length <= 0) {
            return null;
        }
        byte[] result = new byte[length];
        in.readBytes(result);
        String s = new String(result, 0, length);
        return s;
    }

    public static String readStringWithLength(ByteBuf in, String charset) throws UnsupportedEncodingException {
        int length = (int) readLength(in);
        if (length <= 0) {
            return null;
        }
        byte[] result = new byte[length];
        in.readBytes(result);
        String s = new String(result, 0, length, charset);
        return s;
    }

    public static String readStringWithLength(ByteBuf in) {
        int length = (int) readLength(in);
        if (length <= 0) {
            return "";
        }
        byte[] result = new byte[length];
        in.readBytes(result);
        String s = new String(result, 0, length);
        return s;
    }

    public static String readStringWithLength(ByteBuffer in) {
        int length = (int) readLength(in);
        if (length <= 0) {
            return "";
        }
        byte[] result = new byte[length];
        in.get(result);
        String s = new String(result, 0, length);
        return s;
    }

    public static BigDecimal readBigDecimal(ByteBuf in)  {
        String src = readStringWithLength(in);
        return src == null ? null : new BigDecimal(src);
    }

    public static BigDecimal readBigDecimal(ByteBuffer in)  {
        String src = readStringWithLength(in);
        return src == null ? null : new BigDecimal(src);
    }

    @SuppressWarnings("unused")
    public static Duration readTime(ByteBuf in) {
        int length = in.readByte() & 0xff;
        if (length == 0) {
            return Duration.ofSeconds(0);
        }
        int negtive = in.readByte() & 0xff;
        int day = in.readInt();
        int hour = in.readByte() & 0xff;
        int minute = in.readByte() & 0xff;
        int second = in.readByte() & 0xff;
        if (length == 8) {
            return Duration.ofSeconds(hour * 3600 + minute * 60 + second);
        }
        int ms = readInt(in);
        return Duration.ofMillis(hour * 3600 * 1000 + minute * 60 * 1000 + second * 1000 + ms);
    }

    @SuppressWarnings("unused")
    public static Duration readTime(ByteBuffer in) {
        int length = in.get() & 0xff;
        if (length == 0) {
            return Duration.ofSeconds(0);
        }
        int negtive = in.get() & 0xff;
        int day = in.getInt();
        int hour = in.get() & 0xff;
        int minute = in.get() & 0xff;
        int second = in.get() & 0xff;
        if (length == 8) {
            return Duration.ofSeconds(hour * 3600 + minute * 60 + second);
        }
        int ms = readInt(in);
        return Duration.ofMillis(hour * 3600 * 1000 + minute * 60 * 1000 + second * 1000 + ms);
    }

    public static Date readDate(ByteBuf in) {
        Timestamp ts = readTimestamp(in);
        return new Date(ts.getTime());
    }

    public static Date readDate(ByteBuffer in) {
        Timestamp ts = readTimestamp(in);
        return new Date(ts.getTime());
    }

    public static Timestamp readTimestamp(ByteBuf in) {
        byte length = in.readByte();
        int year = readInt(in);
        byte month = readByte(in);
        byte date = readByte(in);
        int hour = readByte(in);
        int minute = readByte(in);
        int second = readByte(in);
        if (length == 11) {
            long nanos = readUB4(in) * 1000;
            Calendar cal = getLocalCalendar();
            cal.set(year, --month, date, hour, minute, second);
            Timestamp time = new Timestamp(cal.getTimeInMillis());
            time.setNanos((int) nanos);
            return time;
        } else {
            Calendar cal = getLocalCalendar();
            cal.set(year, --month, date, hour, minute, second);
            return new Timestamp(cal.getTimeInMillis());
        }
    }

    public static Timestamp readTimestamp(ByteBuffer in) {
        byte length = in.get();
        int year = readInt(in);
        byte month = readByte(in);
        byte date = readByte(in);
        int hour = readByte(in);
        int minute = readByte(in);
        int second = readByte(in);
        if (length == 11) {
            long nanos = readUB4(in) * 1000;
            Calendar cal = getLocalCalendar();
            cal.set(year, --month, date, hour, minute, second);
            Timestamp time = new Timestamp(cal.getTimeInMillis());
            time.setNanos((int) nanos);
            return time;
        } else {
            Calendar cal = getLocalCalendar();
            cal.set(year, --month, date, hour, minute, second);
            return new Timestamp(cal.getTimeInMillis());
        }
    }

    public static float readFloat(ByteBuf in) {
        return Float.intBitsToFloat(readLong(in));
    }

    public static float readFloat(ByteBuffer in) {
        return Float.intBitsToFloat(readLong(in));
    }

    public static byte readByte(ByteBuf in) {
        byte b0 = in.readByte();
        return b0 ;
    }

    public static byte readByte(ByteBuffer in) {
        byte b0 = in.get();
        return b0 ;
    }

    public static long readUB4(ByteBuf in) {
        long l = in.readByte() & 0xff;
        l |= (in.readByte() & 0xff) << 8;
        l |= (in.readByte() & 0xff) << 16;
        l |= (in.readByte() & 0xff) << 24;
        return l;
    }

    public static long readUB4(ByteBuffer in) {
        long l = in.get() & 0xff;
        l |= (in.get() & 0xff) << 8;
        l |= (in.get() & 0xff) << 16;
        l |= (in.get() & 0xff) << 24;
        return l;
    }

/**
    public static byte[] readBytesWithNull(ByteBuf in) {
        final byte[] b = this.data;
        if (position >= length) {
            return EMPTY_BYTES;
        }
        int offset = -1;
        for (int i = position; i < length; i++) {
            if (b[i] == 0) {
                offset = i;
                break;
            }
        }
        switch (offset) {
        case -1:
            byte[] ab1 = new byte[length - position];
            System.arraycopy(b, position, ab1, 0, ab1.length);
            position = length;
            return ab1;
        case 0:
            position++;
            return EMPTY_BYTES;
        default:
            byte[] ab2 = new byte[offset - position];
            System.arraycopy(b, position, ab2, 0, ab2.length);
            position = offset + 1;
            return ab2;
        }
    }
*/
    public static byte[] readBytesWithLength(ByteBuf in) {
        int length = (int) in.readByte();
        if(length==NULL_LENGTH) {
            return null;
        }
        if (length <= 0) {
            return EMPTY_BYTES;
        }

        byte[] ab = new byte[length];
        in.readBytes(ab, 0, length);
        return ab;
    }

    // read bytes with known length
    public static byte[] readBytes(ByteBuf in, int length) {
        if(length==NULL_LENGTH) {
            return null;
        }
        if (length <= 0) {
            return EMPTY_BYTES;
        }

        byte[] ab = new byte[length];
        in.readBytes(ab, 0, length);
        return ab;
    }

    // read bytes with known length
    public static byte[] readBytes(ByteBuffer in, int length) {
        if(length==NULL_LENGTH) {
            return null;
        }
        if (length <= 0) {
            return EMPTY_BYTES;
        }

        byte[] ab = new byte[length];
        in.get(ab, 0, length);
        return ab;
    }

    // handle padding byte for Packed Integers
    public static long readPackedInteger(ByteBuf in, int size) {
    	byte b0 = in.readByte();
    	int length = b0 & 0xff;
        long val;
        
        int count = 1;
        
        switch (length) {
        case 251:
        	val = NULL_LENGTH;
        	break;
        case 252:
            val = (b0 & 0xff) 
            | ((in.readByte() & 0xff) << 8) 
            | ((in.readByte() & 0xff) << 16);
        	count = 3;
        	break;
        case 253:
            val = (b0 & 0xff) 
            | ((in.readByte() & 0xff) << 8) 
            | ((in.readByte() & 0xff) << 16)  
            | ((in.readByte() & 0xff) << 24);
        	count = 4;
        	break;
        case 254:
            val = (b0 & 0xff) 
            | ((in.readByte() & 0xff) << 8) 
            | ((in.readByte() & 0xff) << 16)  
            | ((in.readByte() & 0xff) << 24)  
            | ((in.readByte() & 0xff) << 32)  
            | ((in.readByte() & 0xff) << 40)  
            | ((in.readByte() & 0xff) << 48)  
            | ((in.readByte() & 0xff) << 56)  
            | ((in.readByte() & 0xff) << 64);
        	count = 9;
        	break;
        default:
            val = length;
        }
        
        if (size>count)
        	readBytes(in, size-count);
        	
        return val;
    }

    public static long readLength(ByteBuf in) {
        int length = in.readByte() & 0xff;
        switch (length) {
        case 251:
            return NULL_LENGTH;
        case 252:
            return readInt(in);
        case 253:
            return readLongInt(in);
        case 254:
            return readLongLong(in);
        default:
            return length;
        }
    }

    public static long readLength(ByteBuffer in) {
        int length = in.get() & 0xff;
        switch (length) {
        case 251:
            return NULL_LENGTH;
        case 252:
            return readInt(in);
        case 253:
            return readLongInt(in);
        case 254:
            return readLongLong(in);
        default:
            return length;
        }
    }

    public static final int getLength(long length) {
        if (length < 251) {
            return 1;
        } else if (length < 0x10000L) {
            return 3;
        } else if (length < 0x1000000L) {
            return 4;
        } else {
            return 9;
        }
    }

    public static final int getLength(byte[] src) {
        int length = src.length;
        if (length < 251) {
            return 1 + length;
        } else if (length < 0x10000L) {
            return 3 + length;
        } else if (length < 0x1000000L) {
            return 4 + length;
        } else {
            return 9 + length;
        }
    }

    public static final int getLength(String src) {
        return getLength(src.getBytes());
    }

    public static void writeLengthInt(ByteBuf buf, long value) 
    {
        if (value<251) {
            buf.writeByte((int)value);
        }
        else if (value<Math.pow(2, 16)) {
            buf.writeByte(0xfc);
            buf.writeByte((int)(value & 0xff));
            buf.writeByte((int)(value >>> 8));
        }
        else if (value<Math.pow(2, 24)) {
            buf.writeByte(0xfd);
            buf.writeByte((int)(value & 0xff));
            buf.writeByte((int)(value >>> 8));
            buf.writeByte((int)(value >>> 16));
        }
        else if (value<Math.pow(2, 64))
        {
            buf.writeByte(0xfe);
            buf.writeByte((int)(value & 0xff));
            buf.writeByte((int)(value >>> 8));
            buf.writeByte((int)(value >>> 16));
            buf.writeByte((int)(value >>> 24));
            buf.writeByte((int)(value >>> 32));
            buf.writeByte((int)(value >>> 40));
            buf.writeByte((int)(value >>> 48));
            buf.writeByte((int)(value >>> 56));
        };
    }

    public static final void writeWithNull(ByteBuf buffer, byte[] src) {
        if (src!=null) {
            buffer.writeBytes(src);
        }
        buffer.writeByte((byte) 0);
    }

    private static final ThreadLocal<Calendar> localCalendar = new ThreadLocal<Calendar>();

    private static final Calendar getLocalCalendar() {
        Calendar cal = localCalendar.get();
        if (cal == null) {
            cal = Calendar.getInstance();
            localCalendar.set(cal);
        }
        return cal;
    }

    /**
     * decoding string from mysql is not easy. literals is encoded in utf8. chances are some lierals are encoded in 
     * binary. we need this pretty logic to convert mysql binary to string
     * @param buf
     * @return
     */
    public static CharBuffer readStringCrazy(Decoder decoder, ByteBuf buf) {
        	CharBuffer cbuf = MysqlString.decode(decoder, buf.nioBuffer());
        	cbuf.flip();
        	return cbuf;
    }

}