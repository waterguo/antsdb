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

import org.apache.commons.lang.NotImplementedException;
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

    public static int readLong(ByteBuf in) {
        int l = (int) (in.readByte() & 0xff);
        l |= (int) (in.readByte() & 0xff) << 8;
        l |= (int) (in.readByte() & 0xff) << 16;
        l |= (int) (in.readByte() & 0xff) << 24;
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

    public static double readDouble(ByteBuf in) {
        return Double.longBitsToDouble(readLongLong(in));
    }

    public static int readInt(ByteBuf in) {
        byte b0 = in.readByte();
        byte b1 = in.readByte();
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

    public static BigDecimal readBigDecimal(ByteBuf in)  {
        String src = readStringWithLength(in);
        return src == null ? null : new BigDecimal(src);
    }

    public static Duration readTime(ByteBuf in) {
        in.readBytes(6);
        int hour = readInt(in);
        int minute = readInt(in);
        int second = readInt(in);
        return Duration.ofSeconds(hour * 3600 + minute * 60 + second);
    }

    public static Date readDate(ByteBuf in) {
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

    public static void writeTimestamp(ByteBuf buf, Timestamp date) {
        buf.writeByte(11);
        if (date.getTime() == Long.MIN_VALUE) {
            // 0 datetime in mysql
            BufferUtils.writeUB2(buf, 0);
            buf.writeByte(0);
            buf.writeByte(0);
            buf.writeByte(0);
            buf.writeByte(0);
            buf.writeByte(0);
            BufferUtils.writeUB4(buf, 0);
        }
        else {
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            BufferUtils.writeUB2(buf, cal.get(Calendar.YEAR));
            buf.writeByte(cal.get(Calendar.MONTH)+1);
            buf.writeByte(cal.get(Calendar.DAY_OF_MONTH));
            buf.writeByte(cal.get(Calendar.HOUR_OF_DAY));
            buf.writeByte(cal.get(Calendar.MINUTE));
            buf.writeByte(cal.get(Calendar.SECOND));
            BufferUtils.writeUB4(buf, cal.get(Calendar.MILLISECOND)*1000);
        }
    }

    public static void writeDate(ByteBuf buf, java.util.Date date) {
        buf.writeByte(7);
        if (date.getTime() == Long.MIN_VALUE) {
            // 0 date in mysql
            BufferUtils.writeUB2(buf, 0);
            buf.writeByte(0);
            buf.writeByte(0);
            buf.writeByte(0);
            buf.writeByte(0);
            buf.writeByte(0);
        }
        else {
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            BufferUtils.writeUB2(buf, cal.get(Calendar.YEAR));
            buf.writeByte(cal.get(Calendar.MONTH)+1);
            buf.writeByte(cal.get(Calendar.DAY_OF_MONTH));
            buf.writeByte(cal.get(Calendar.HOUR_OF_DAY));
            buf.writeByte(cal.get(Calendar.MINUTE));
            buf.writeByte(cal.get(Calendar.SECOND));
        }
    }


    public static float readFloat(ByteBuf in) {
        return Float.intBitsToFloat(readLong(in));
    }

    public static byte readByte(ByteBuf in) {
        byte b0 = in.readByte();
        return b0 ;
    }

    public static long readUB4(ByteBuf in) {
        long l = in.readByte() & 0xff;
        l |= (in.readByte() & 0xff) << 8;
        l |= (in.readByte() & 0xff) << 16;
        l |= (in.readByte() & 0xff) << 24;
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

    public static final void writeLength(ByteBuf buffer, long l) {
        if (l < 251) {
            buffer.writeByte((byte) l);
        } else if (l < 0x10000L) {
            buffer.writeByte((byte) 252);
            writeUB2(buffer, (int) l);
        } else if (l < 0x1000000L) {
            buffer.writeByte((byte) 253);
            writeUB3(buffer, (int) l);
        } else {
            buffer.writeByte((byte) 254);
            writeUB4(buffer, l);
        }
    }

    public static final void writeUB2(ByteBuf buf, int i) {
        buf.writeByte((byte) (i & 0xff));
        buf.writeByte((byte) (i >>> 8));
    }

    public static final void writeUB3(ByteBuf buf, int i) {
        buf.writeByte((byte) (i & 0xff));
        buf.writeByte((byte) (i >>> 8));
        buf.writeByte((byte) (i >>> 16));
    }

    public static final void writeUB4(ByteBuf buf, long l) {
        buf.writeByte((byte) (l & 0xff));
        buf.writeByte((byte) (l >>> 8));
        buf.writeByte((byte) (l >>> 16));
        buf.writeByte((byte) (l >>> 24));
    }

    public static void writeLongInt(ByteBuf buf, int length) {
        buf.writeByte((byte) (length & 0xff));
        buf.writeByte((byte) (length >>> 8));
        buf.writeByte((byte) (length >>> 16));
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

    public static void writeInt(ByteBuf buf, int i) {
        buf.writeByte((byte) (i & 0xff));
        buf.writeByte((byte) (i >>> 8));
    }

    public static void writeLong(ByteBuf buf, long i) {
        buf.writeByte((byte) (i & 0xff));
        buf.writeByte((byte) (i >>> 8));
        buf.writeByte((byte) (i >>> 16));
        buf.writeByte((byte) (i >>> 24));
    }
    
    public static void writeFieldLength(ByteBuf buf, long length) {
        if (length < 251) {
            buf.writeByte((byte) length);
        } 
        else if (length < 0x10000L) {
            buf.writeByte((byte) 252);
            writeUB2(buf, (int) length);
        } 
        else if (length < 0x1000000L) {
            buf.writeByte((byte) 253);
            writeUB3(buf, (int) length);
        } 
        else {
            buf.writeByte((byte) 254);
            writeLong(buf, length);
        }
    }

    public static void writeLongLong(ByteBuf buf, long i) {
        buf.writeByte((byte) (i & 0xff));
        buf.writeByte((byte) (i >>> 8));
        buf.writeByte((byte) (i >>> 16));
        buf.writeByte((byte) (i >>> 24));
        buf.writeByte((byte) (i >>> 32));
        buf.writeByte((byte) (i >>> 40));
        buf.writeByte((byte) (i >>> 48));
        buf.writeByte((byte) (i >>> 56));
    }

    public static void writeLenString(ByteBuf buf, String s, Charset encoder) {
        if (s == null) {
            writeFieldLength(buf, 0);
            return;
        }
        ByteBuffer bb = encoder.encode(s);
        writeFieldLength(buf, bb.remaining());
        buf.writeBytes(bb);
    }

    public static void writeLenString(ByteBuf buf, long pValue) {
    	if (pValue == 0) {
            writeFieldLength(buf, 0);
            return;
    	}
    	throw new NotImplementedException();
    }
    
    public static final void writeWithLength(ByteBuf buffer, byte[] src, byte nullValue) {
        if (src == null) {
            buffer.writeByte(nullValue);
        } else {
            writeWithLength(buffer, src);
        }
    }

    public static final void writeWithLength(ByteBuf buffer, byte[] src) {
        if (src==null || src.length==0)
        {
            buffer.writeByte((byte)0);
        }
        else
        {
            int length = src==null? 0: src.length;
            if (length < 251) {
                buffer.writeByte((byte) length);
            } else if (length < 0x10000L) {
                buffer.writeByte((byte) 252);
                writeUB2(buffer, length);
            } else if (length < 0x1000000L) {
                buffer.writeByte((byte) 253);
                writeUB3(buffer, length);
            } else {
                buffer.writeByte((byte) 254);
                writeLong(buffer, length);
            }
            buffer.writeBytes(src);
        }
    }

    public static final void writeWithNull(ByteBuf buffer, byte[] src) {
        if (src!=null)
        {
            buffer.writeBytes(src);
        }
        buffer.writeByte((byte) 0);
    }

    private static void writeStringNoNull(ByteBuf buf, String s) {
        Charset cs = Charset.defaultCharset();
        buf.writeBytes(cs.encode(s));
    }
    
    public static void writeString(ByteBuf buf, String s) {
        writeStringNoNull(buf, s);
        buf.writeByte(0);
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