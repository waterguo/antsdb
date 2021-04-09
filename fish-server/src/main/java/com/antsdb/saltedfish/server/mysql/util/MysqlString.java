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

import java.nio.ByteBuffer;
import java.nio.CharBuffer;

import com.antsdb.saltedfish.charset.Decoder;
import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * crazy logic to decode mysql query. unbelievable that i need to implement utf8 by myself 
 *  
 * @author wgu0
 */
public final class MysqlString {
    final static class Buffer {
        long addr;
        int length;
        int pos = 0;
        Decoder decoder;
        
        Buffer(Decoder decoder, ByteBuffer buf) {
            if (!buf.isDirect()) {
                throw new IllegalArgumentException();
            }
            this.length = buf.remaining();
            this.addr = UberUtil.getAddress(buf) + buf.position();
            this.decoder = decoder;
        }
        
        Buffer(Decoder decoder, long addr, int length) {
            this.addr = addr;
            this.length = length;
            this.decoder = decoder;
        }

        public int remaining() {
            return this.length - this.pos;
        }

        public char getChar() {
            if (this.pos >= this.length) {
                throw new IllegalArgumentException();
            }
            int ch = this.decoder.getChar(this.addr + this.pos);
            this.pos += ch >> 16;
            return (char)ch;
        }

        public byte getByte() {
            if (this.pos >= this.length) {
                throw new IllegalArgumentException();
            }
            byte result = Unsafe.getByte(this.addr + this.pos);
            this.pos++;
            return result;
        }
        
        public int position() {
            return this.pos;
        }
        
        public void position(int pos) {
            this.pos = pos;
        }

        public boolean hasRemaining() {
            return remaining() > 0;
        }

    }
    
    public static CharBuffer decode(Decoder decoder, ByteBuffer buf) {
        return decode(new Buffer(decoder, buf));
    }
    
    public static CharBuffer decode(Decoder decoder, long addr, int length) {
        return decode(new Buffer(decoder, addr, length));
    }
    
    static CharBuffer decode(Buffer buf) {
        CharBuffer cbuf = CharBuffer.allocate(buf.remaining());
        while (buf.remaining() > 0) {
            if (readMysqlExtension(buf, cbuf)) {
                continue;
            }
            char ch = (char)buf.getChar();
            cbuf.put(ch);
            if (ch == '\\') {
                readByte(buf, cbuf);
            }
            else if (ch == '\'') {
                int pos = buf.position();
                cbuf.mark();
                if (readLiteral(buf, cbuf, ch)) {
                    continue;
                }
                buf.position(pos);
                cbuf.reset();
            }
            else if (ch == '"') {
                int pos = buf.position();
                cbuf.mark();
                if (readLiteral(buf, cbuf, ch)) {
                    continue;
                }
                buf.position(pos);
                cbuf.reset();
            }
            else if (ch == '_') {
                int pos = buf.position();
                cbuf.mark();
                if (readBinary(buf, cbuf)) {
                    continue;
                }
                buf.position(pos);
                cbuf.reset();
            }
        }
        return cbuf;
    }

    /*
     * https://dev.mysql.com/doc/refman/5.7/en/comments.html
     * 
     * mysql extensions starts with "/*!", end with * /
     */
    private static boolean readMysqlExtension(Buffer buf, CharBuffer cbuf) {
        // check leading mark
        
        if (!skipToken(buf, "/*!")) {
            return false;
        }
        skipUntil(buf, ' ');
        
        // read stuff
        
        while (buf.hasRemaining()) {
            if (skipToken(buf, "*/")) {
                break;
            }
            char ch = (char)buf.getChar();
            cbuf.put(ch);
        }
        return true;
    }
    
    private static boolean skipToken(Buffer buf, String token) {
        int mark = buf.position();
        int i=0; 
        while (buf.hasRemaining() && (i < token.length())) {
            int ch = buf.getChar();
            if (ch != token.charAt(i)) {
                break;
            }
            i++;
        }
        if (i == token.length()) {
            return true;
        }
        else {
            buf.position(mark);
            return false;
        }
    }
    
    private static void skipUntil(Buffer buf, char end) {
        while (buf.hasRemaining()) {
            int mark = buf.position();
            int ch = buf.getChar();
            if (ch == end) {
                buf.position(mark);
                return;
            }
        }
    }
    
    private static boolean readBinary(Buffer buf, CharBuffer cbuf) {
        if (!readByteIf('b', 'B', buf, cbuf)) {
            return false;
        }
        if (!readByteIf('i', 'I', buf, cbuf)) {
            return false;
        }
        if (!readByteIf('n', 'N', buf, cbuf)) {
            return false;
        }
        if (!readByteIf('a', 'A', buf, cbuf)) {
            return false;
        }
        if (!readByteIf('r', 'R', buf, cbuf)) {
            return false;
        }
        if (!readByteIf('y', 'Y', buf, cbuf)) {
            return false;
        }
        if (!readByteIf('\'', '\'', buf, cbuf)) {
            return false;
        }
        while (buf.remaining() > 0) {
            char ch = (char)(buf.getByte() & 0xff);
            cbuf.put(ch);
            if (ch == '\\') {
                readByte(buf, cbuf);
            }
            else if (ch == '\'') {
                break;
            }
        }
        return true;
    }

    private static boolean readLiteral(Buffer buf, CharBuffer cbuf, int end) {
        while (buf.remaining() > 0) {
            // mysql uses binary string
            char ch = (char)(buf.getByte() & 0xff);
            cbuf.put(ch);
            if (ch == '\\') {
                readByte(buf, cbuf);
            }
            else if (ch == end) {
                break;
            }
        }
        return true;
    }

    private static void readByte(Buffer buf, CharBuffer cbuf) {
        if (buf.remaining() > 0) {
            char ch = (char)(buf.getByte() & 0xff);
            cbuf.put(ch);
        }
    }

    private static boolean readByteIf(char ch1, char ch2, Buffer buf, CharBuffer cbuf) {
        if (buf.remaining() > 0) {
            char ch = (char)buf.getChar();
            if ((ch == ch1) || (ch == ch2)) {
                cbuf.put(ch);
                return true;
            }
        }
        return false;
    }

}
