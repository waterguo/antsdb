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
package com.antsdb.saltedfish.sql.vdm;

import com.antsdb.saltedfish.cpp.FlexibleHeap;
import com.antsdb.saltedfish.cpp.Heap;

/**
 * cursor with built-in heap. it is more convenient than using Cursor directly. 
 *  
 * @author wgu0
 */
public abstract class CursorWithHeap extends Cursor {
	private Heap heap;
	private long pRecord;
	private long heapMark;
	
	public CursorWithHeap(CursorMeta meta) {
		super(meta);
		this.heap = new FlexibleHeap();
		this.pRecord = Record.alloc(heap, meta.getColumnCount());
		this.heapMark = this.heap.position();
	}

	public CursorWithHeap(CursorMaker maker) {
		super(maker);
		this.heap = new FlexibleHeap();
		this.pRecord = Record.alloc(heap, meta.getColumnCount());
		this.heapMark = this.heap.position();
	}

	@Override
	public void close() {
		if (this.heap != null) {
			this.heap.free();
		}
		this.heap = null;
	}
	
	protected Heap newHeap() {
		this.heap.reset(heapMark);
		return this.heap;
	}

	protected Heap getHeap() {
		return this.heap;
	}
	
	protected long newRecord() {
		Record.reset(this.pRecord);
		return this.pRecord;
	}
}
