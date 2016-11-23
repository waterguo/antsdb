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
package com.antsdb.saltedfish.nosql;

import com.antsdb.saltedfish.cpp.BluntHeap;
import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.cpp.Unsafe;

/**
 * 
 * @author wgu0
 */
public class MutableRow extends Row {
	BluntHeap heap;
			
	public MutableRow(BluntHeap heap, int maxColumnId) {
		super(heap.alloc(getHeaderSize(maxColumnId-1)));
		this.heap = heap;
    	Unsafe.putByte(this.addr, FILE_VERSION);
    	Unsafe.putShort(this.addr + OFFSET_MAX_COLUMN_ID, (short)maxColumnId);
    	updateSize();
	}

	public MutableRow setTableId(int tableId) {
    	Unsafe.putInt(this.addr + OFFSET_TABLE_ID, tableId);
    	return this;
	}
	
	public void setVersion(long value) {
		super.setVersion(value);
    	Unsafe.putLong(this.addr + OFFSET_TRX_TS, value);
	}

	public MutableRow setKey(byte[] value) {
		KeyBytes key = KeyBytes.allocSet(heap, value);
		long pKey = key.getAddress();
    	Unsafe.putInt(this.addr + OFFSET_KEY_OFFSET, (int)(pKey - this.addr));
    	updateSize();
    	return this;
	}
	
	public MutableRow set(int column, Object value) {
		if (value == null) {
    		Unsafe.putInt(this.addr + OFFSET_VALUES_OFFSETS + column * 4, 0);
    		return this;
		}
		long pValue = FishObject.allocSet(heap, value);
		int offset = (int)(pValue - this.addr);
		Unsafe.putInt(this.addr + OFFSET_VALUES_OFFSETS + column * 4, offset);
		updateSize();
		return this;
	}
	
	private void updateSize() {
		int size = (int)(this.heap.getAddress((int)heap.position()) - this.addr);
    	Unsafe.putInt3(this.addr + OFFSET_LENGTH, size);
	}
}
