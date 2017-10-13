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
package com.antsdb.saltedfish.server.mysql.packet;

import java.nio.CharBuffer;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.IntStream;
import org.antlr.v4.runtime.misc.Interval;

/**
 * CharStream made for antlr
 *  
 * @author wgu0
 */
class MysqlCharStream implements CharStream {
	/** How many characters are actually in the buffer */
	protected int n;

	/** 0..n-1 index into string of next char */
	protected int p=0;
	
	CharBuffer buf;
	
	MysqlCharStream(CharBuffer buf) {
		this.buf = buf.slice();
		this.n = this.buf.limit();
	}
	
	@Override
	public void consume() {
		if (p >= n) {
			assert LA(1) == IntStream.EOF;
			throw new IllegalStateException("cannot consume EOF");
		}

        if ( p < n ) {
            p++;
        }
	}

	@Override
	public int LA(int i) {
		if ( i==0 ) {
			return 0; // undefined
		}
		if ( i<0 ) {
			i++; // e.g., translate LA(-1) to use offset i=0; then data[p+0-1]
			if ( (p+i-1) < 0 ) {
				return IntStream.EOF; // invalid; no char before first char
			}
		}

		if ( (p+i-1) >= n ) {
            return IntStream.EOF;
        }
		int ch = this.buf.get(p+i-1);
		return ch;
	}

	@Override
	public int mark() {
		return -1;
	}

	@Override
	public void release(int marker) {
	}

	@Override
	public int index() {
		return p;
	}

	@Override
	public void seek(int index) {
		if ( index<=p ) {
			p = index; // just jump; don't update stream state (line, ...)
			return;
		}
		// seek forward, consume until p hits index or n (whichever comes first)
		index = Math.min(index, n);
		while ( p<index ) {
			consume();
		}
	}

	@Override
	public int size() {
		return this.buf.limit();
	}

	@Override
	public String getSourceName() {
		return "mysql protocol";
	}

	@Override
	public String getText(Interval interval) {
		StringBuffer sbuf = new StringBuffer();
		for (int i=interval.a; i<=interval.b; i++) {
			sbuf.append((char)this.buf.get(i));
		}
		return sbuf.toString();
	}

	@Override
	public String toString() {
		StringBuffer sbuf = new StringBuffer();
		for (int i=0; i<this.buf.limit(); i++) {
			int ch = (char)this.buf.get(i);
			sbuf.append((char)ch);
		}
		return sbuf.toString();
	}
	
}
