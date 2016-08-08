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

import java.nio.ByteBuffer;
import java.nio.CharBuffer;

import com.antsdb.saltedfish.charset.Decoder;

/**
 * crazy logic to decode mysql query. unbelievable that i need to implement utf8 by myself 
 *  
 * @author wgu0
 */
class MysqlString {
	static CharBuffer decode(Decoder decoder, ByteBuffer buf) {
		CharBuffer cbuf = CharBuffer.allocate(buf.remaining());
		while (buf.remaining() > 0) {
			char ch = (char)decoder.get(buf);
			cbuf.put(ch);
			if (ch == '\\') {
				readByte(decoder, buf, cbuf);
			}
			else if (ch == '\'') {
				buf.mark();
				cbuf.mark();
				if (readLiteral(decoder, buf, cbuf)) {
					continue;
				}
				buf.reset();
				cbuf.reset();
			}
			else if (ch == '_') {
				buf.mark();
				cbuf.mark();
				if (readBinary(decoder, buf, cbuf)) {
					continue;
				}
				buf.reset();
				cbuf.reset();
			}
		}
		return cbuf;
	}

	private static boolean readBinary(Decoder decoder, ByteBuffer buf, CharBuffer cbuf) {
		if (!readByteIf(decoder, 'b', 'B', buf, cbuf)) {
			return false;
		}
		if (!readByteIf(decoder, 'i', 'I', buf, cbuf)) {
			return false;
		}
		if (!readByteIf(decoder, 'n', 'N', buf, cbuf)) {
			return false;
		}
		if (!readByteIf(decoder, 'a', 'A', buf, cbuf)) {
			return false;
		}
		if (!readByteIf(decoder, 'r', 'R', buf, cbuf)) {
			return false;
		}
		if (!readByteIf(decoder, 'y', 'Y', buf, cbuf)) {
			return false;
		}
		if (!readByteIf(decoder, '\'', '\'', buf, cbuf)) {
			return false;
		}
		while (buf.remaining() > 0) {
			char ch = (char)(buf.get() & 0xff);
			cbuf.put(ch);
			if (ch == '\\') {
				readByte(decoder, buf, cbuf);
			}
			else if (ch == '\'') {
				break;
			}
		}
		return true;
	}

	private static boolean readLiteral(Decoder decoder, ByteBuffer buf, CharBuffer cbuf) {
		while (buf.remaining() > 0) {
			char ch = (char)decoder.get(buf);
			cbuf.put(ch);
			if (ch == '\\') {
				readByte(decoder, buf, cbuf);
			}
			else if (ch == '\'') {
				break;
			}
		}
		return true;
	}

	private static void readByte(Decoder decoder, ByteBuffer buf, CharBuffer cbuf) {
		if (buf.remaining() > 0) {
			char ch = (char)(buf.get() & 0xff);
			cbuf.put(ch);
		}
	}

	private static boolean readByteIf(Decoder decoder, char ch1, char ch2, ByteBuffer buf, CharBuffer cbuf) {
		if (buf.remaining() > 0) {
			char ch = (char)decoder.get(buf);
			if ((ch == ch1) || (ch == ch2)) {
				cbuf.put(ch);
				return true;
			}
		}
		return false;
	}

}
