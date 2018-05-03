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
package com.antsdb.saltedfish.charset;

import java.nio.ByteBuffer;
import java.util.function.IntConsumer;
import java.util.function.IntSupplier;

import com.antsdb.saltedfish.cpp.Unsafe;

/**
 * Utf8 decoder
 *  
 * @author wgu0
 */
public final class Utf8 implements Decoder {

	@Override
	public int get(ByteBuffer buf) {
		int ch = buf.get() & 0xff;
		if ((ch & 0x7f) == 0) {
		}
		else if ((ch & 0xe0) == 0xc0) {
			// 2 bytes utf
			if (buf.remaining() >= 1) {
				ch = (ch & 0x1f) << 6;
				ch = ch | (buf.get() & 0x3f);
			}
		}
		else if ((ch & 0xf0) == 0xe0) {
			// 3 bytes utf
			if (buf.remaining() >= 2) {
				ch = (ch & 0xf) << 12;
				ch = ch | ((buf.get() & 0x3f) << 6);
				ch = ch | (buf.get() & 0x3f);
			}
		}
		else if ((ch & 0xf8) == 0xf0) {
			// 4 bytes utf
			if (buf.remaining() >= 3) {
				ch = (ch & 0x7) << 18;
				ch = ch | ((buf.get() & 0x3f) << 12);
				ch = ch | ((buf.get() & 0x3f) << 6);
				ch = ch | (buf.get() & 0x3f);
			}
		}
		return ch;
	}

	public int get(IntSupplier supplier) {
		int ch = supplier.getAsInt();
		if (ch == -1) {
			return -1;
		}
		ch = ch & 0xff;
		if ((ch & 0x80) == 0) {
			return ch;
		}
		else if ((ch & 0xe0) == 0xc0) {
			// 2 bytes utf
			int next = supplier.getAsInt();
			ch = (ch & 0x1f) << 6;
			ch = ch | (next & 0x3f);
			return ch;
		}
		else if ((ch & 0xf0) == 0xe0) {
			// 3 bytes utf
			int next = supplier.getAsInt();
			int nextnext = supplier.getAsInt();
			ch = (ch & 0xf) << 12;
			ch = ch | ((next & 0x3f) << 6);
			ch = ch | (nextnext & 0x3f);
			return ch;
		}
		else if ((ch & 0xf8) == 0xf0) {
			// 4 bytes utf
			int next = supplier.getAsInt();
			int nextnext = supplier.getAsInt();
			int nextnextnext = supplier.getAsInt();
			ch = (ch & 0x7) << 18;
			ch = ch | ((next & 0x3f) << 12);
			ch = ch | ((nextnext & 0x3f) << 6);
			ch = ch | (nextnextnext & 0x3f);
		}
		else if (ch == -1) {
			return -1;
		}
		throw new IllegalArgumentException();
	}
	
    public IntSupplier mapDecode(IntSupplier supplier) {
        IntSupplier result = new IntSupplier() {
            @Override
            public int getAsInt() {
                int ch = get(supplier);
                return ch;
            }
        };
        return result;
    }
    
	public String decode(IntSupplier supplier) {
		StringBuilder buf = new StringBuilder();
		IntSupplier output = mapDecode(supplier);
		for (int ch = output.getAsInt(); ch != -1; ch=output.getAsInt()) {
	           buf.append((char)ch); 
		}
		return buf.toString();
	}

    public static void encode(IntSupplier supplier, IntConsumer consumer) {
        for (int ch=supplier.getAsInt(); ch!=-1; ch=supplier.getAsInt()) {
            encode(ch, consumer);
        }
    }
    
	public static void encode(String s, IntConsumer consumer) {
		for (int i=0; i<s.length(); i++) {
			encode(s.charAt(i), consumer);
		}
	}
	
	/**
	 * @see org.apache.hadoop.io.UTF8
	 * @param ch
	 * @param consumer
	 */
	public static void encode(int ch, IntConsumer consumer) {
		if (ch <= 0x7F) {
			consumer.accept(ch);
			return;
		}
		else if (ch <= 0x07FF) {
			consumer.accept((byte) (0xC0 | ((ch >> 6) & 0x1F)));
			consumer.accept((byte) (0x80 | ch & 0x3F));
		}
		else {
			consumer.accept((byte) (0xE0 | ((ch >> 12) & 0X0F)));
			consumer.accept((byte) (0x80 | ((ch >> 6) & 0x3F)));
			consumer.accept((byte) (0x80 | (ch & 0x3F)));
		}
	}

    public static String decode(long pBytes, int size) {
        char[] buf = new char[size];
        int j = 0;
        for (int i=0; i<size;) {
            if (size - i > 8) {
                long value = Unsafe.getLong(pBytes + i);
                if (isAscii(value)) {
                    buf[j++] = (char)(value & 0x7f);
                    buf[j++] = (char)((value >> 8) & 0x7f);
                    buf[j++] = (char)((value >> 16) & 0x7f);
                    buf[j++] = (char)((value >> 24) & 0x7f);
                    buf[j++] = (char)((value >> 32) & 0x7f);
                    buf[j++] = (char)((value >> 40) & 0x7f);
                    buf[j++] = (char)((value >> 48) & 0x7f);
                    buf[j++] = (char)((value >> 56) & 0x7f);
                    i += 8;
                    continue;
                }
            }
            int ch = Unsafe.getByte(pBytes + i);
            if ((ch & 0x80) == 0) {
                buf[j++] = (char)ch;
                i += 1;
                continue;
            }
            else if ((ch & 0xe0) == 0xc0) {
                // 2 bytes utf
                int next = Unsafe.getByte(pBytes + i + 1);
                ch = (ch & 0x1f) << 6;
                ch = ch | (next & 0x3f);
                buf[j++] = (char)ch;
                i += 2;
                continue;
            }
            else if ((ch & 0xf0) == 0xe0) {
                // 3 bytes utf
                int next = Unsafe.getByte(pBytes + i + 1);
                int nextnext = Unsafe.getByte(pBytes + i + 2);
                ch = (ch & 0xf) << 12;
                ch = ch | ((next & 0x3f) << 6);
                ch = ch | (nextnext & 0x3f);
                buf[j++] = (char)ch;
                i += 3;
                continue;
            }
            else if ((ch & 0xf8) == 0xf0) {
                // 4 bytes utf
                int next = Unsafe.getByte(pBytes + i + 1);
                int nextnext = Unsafe.getByte(pBytes + i + 2);
                int nextnextnext = Unsafe.getByte(pBytes + i + 3);
                ch = (ch & 0x7) << 18;
                ch = ch | ((next & 0x3f) << 12);
                ch = ch | ((nextnext & 0x3f) << 6);
                ch = ch | (nextnextnext & 0x3f);
                buf[j++] = (char)ch;
                i += 4;
                continue;
            }
            else {
                buf[j++] = (char)ch;
                i += 1;
                continue;
            }
        }
        return String.valueOf(buf, 0, j);
    }
    
    private static boolean isAscii(long value) {
        return (value & 0x8080808080808080l) == 0;
    }
}
