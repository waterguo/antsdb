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

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;

import com.antsdb.saltedfish.cpp.BluntHeap;
import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.cpp.MurmurHash;
import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.cpp.Value;
import com.antsdb.saltedfish.util.BytesUtil;
import com.antsdb.saltedfish.util.CodingError;
import com.antsdb.saltedfish.util.UberFormatter;
import com.antsdb.saltedfish.util.UberObject;

/**
 * Row is a read-only view of the raw data stored in the file
 * 
 * data structure of a raw row
 * 
 * 0: byte, version
 * 1: byte, tomb stone indicator
 * 2-3: reserved
 * 4: int8, transaction time stamp/transaction id
 * 12: int2, number of columns
 * 16: key offset
 * 20: field offset vector
 * @author xinyi
 *
 */
public class Row extends UberObject implements Map<Integer, Object> {
    public static final int DELETE_MARK = 1;
    protected static final Row TOMB_STONE = new Row(Unsafe.allocateMemory(100));
	protected static final int OFFSET_FORMAT = 0;
    protected static final int OFFSET_MAX_COLUMN_ID = 0x2;
	protected static final int OFFSET_LENGTH = 4;
	protected static final int OFFSET_TRX_TS = 0x8;
    protected static final int OFFSET_HASH = 0x10;
	protected static final int OFFSET_KEY_OFFSET = 0x18;
	protected static final int OFFSET_VALUES_OFFSETS = 0x1c;
    		
	protected static final byte FILE_VERSION = Value.FORMAT_ROW;
    
    transient int tableId;

    long addr;
    int maxColumnid;
    long version;
    
    Row(long addr, long version) {
    	if (Unsafe.getByteVolatile(addr + OFFSET_FORMAT) != FILE_VERSION) {
    		throw new IllegalArgumentException();
    	}
    	this.version = version;
    	this.addr = addr;
    	this.maxColumnid = getMaxColumnId();
    }
    
    protected Row(long addr) {
    	this.addr = addr;
    	this.version = 0;
    	this.maxColumnid = -1;
    }
    
    public boolean isTombStone() {
    	return this == TOMB_STONE;
    }
    
    final static long from(long p, VaporizingRow from) {
    	int size = from.getSize();
    	long pRow = p;
    	int headerSize = getHeaderSize(from.getMaxColumnId());
    	long pData = pRow + headerSize;
    	Unsafe.putByte(pRow, FILE_VERSION);
    	Unsafe.putInt3(pRow + OFFSET_LENGTH, size);
    	Unsafe.putLong(pRow + OFFSET_TRX_TS, from.getTrxTimestamp());
    	Unsafe.putShort(pRow + OFFSET_MAX_COLUMN_ID, (short)from.getMaxColumnId());
    	Unsafe.putInt(pRow + OFFSET_KEY_OFFSET, (int)(pData - pRow));
    	pData = copyValue(from.getKeyAddress(), pData);
    	for (int i=0; i<=from.getMaxColumnId(); i++) {
    		long pValue = from.getFieldAddress(i);
    		if (pValue == 0) {
        		Unsafe.putInt(pRow + OFFSET_VALUES_OFFSETS + i * 4, 0);
        		continue;
    		}
    		int offset = (int)(pData - pRow);
    		Unsafe.putInt(pRow + OFFSET_VALUES_OFFSETS + i * 4, offset);
    		pData = copyValue(pValue, pData);
    	}
    	if ((pData - pRow) != size) {
    		throw new CodingError();
    	}
    	long hash = MurmurHash.hash64(pRow + headerSize, size - headerSize);
    	Unsafe.putLong(pRow + OFFSET_HASH, hash);
    	return pRow;
    }
    
    public final static long from(Heap heap, VaporizingRow from) {
        int size = from.getSize();
        long pRow = heap.alloc(size);
        from(pRow, from);
        return pRow;
    }
    
    private static long copyValue(long pValue, long pData) {
    	if (pValue == 0) {
    		return pData;
    	}
    	int size = FishObject.getSize(pValue);
    	Unsafe.copyMemory(pValue, pData, size);
		return pData + size;
	}

	/**
     * @param pRow
     * @param version 0 means getting the version from row
     */
    public static Row fromMemoryPointer(long pRow, long version) {
    	if (pRow == 0) {
    		return null;
    	}
    	if (pRow == 1) {
    		return TOMB_STONE;
    	}
    	if (version == 0) {
    	    version = Row.getVersion(pRow);
    	}
    	Row row = new Row(pRow, version);
    	return row;
    }
    
    public static Row fromSpacePointer(SpaceManager memman, long spRow, long version) {
    	if (spRow == 0) {
    		return null;
    	}
    	if (spRow == 1) {
    		return TOMB_STONE;
    	}
    	long pRow = memman.toMemory(spRow);
    	return fromMemoryPointer(pRow, version);
    }
    
	public int getKeyOffset() {
        int offset = Unsafe.getIntVolatile(this.addr + OFFSET_KEY_OFFSET);
		return offset;
	}
	
	public long getKeyAddress() {
        int offset = getKeyOffset() & 0xffff;
        return this.addr + offset;
	}

    public byte[] getKey() {
    	int offset = Unsafe.getInt(this.addr + OFFSET_KEY_OFFSET);
    	if (offset == 0) {
    		return null;
    	}
    	if (offset >= 1000) {
    		throw new IllegalArgumentException(String.format("0x%016x", this.addr));
    	}
        byte[] bytes = KeyBytes.create(this.addr + offset).get();
        return bytes;
    }
    
    public Object get(int field) {
    	long addr = getFieldAddress(field);
        Object val = FishObject.get(null, addr);
        return val;
    }

    @Override
    public int size() {
    	int count = 0;
    	for (int i=0; i<=this.maxColumnid; i++) {
    		long addr = getFieldAddress(i);
    		if (addr != 0) {
    			count++;
    		}
    	}
        return count;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean containsKey(Object key) {
    	return keySet().contains(key);
    }

    @Override
    public boolean containsValue(Object value) {
    	return values().contains(value);
    }

    @Override
    public Object get(Object key) {
        return get((int)key);
    }

    @Override
    public Object put(Integer key, Object value) {
    	throw new NotImplementedException();
    }

    @Override
    public Object remove(Object key) {
    	throw new NotImplementedException();
    }

	@Override
    public void putAll(Map<? extends Integer, ? extends Object> m) {
    	throw new NotImplementedException();
    }

    @Override
    public void clear() {
    	throw new NotImplementedException();
    }

    @Override
    public Set<Integer> keySet() {
    	Set<Integer> set = new LinkedHashSet<>();
    	int maxColumnId = getMaxColumnId();
    	for (int i=0; i<maxColumnId; i++) {
    		if (getFieldAddress(i) != 0) {
    			set.add(i);
    		}
    	}
        return set;
    }

    @Override
    public Collection<Object> values() {
    	Set<Object> set = new LinkedHashSet<>();
    	int maxColumnId = getMaxColumnId();
    	for (int i=0; i<maxColumnId; i++) {
    		Object value = get(i);
    		if (value != null) {
    			set.add(value);
    		}
    	}
        return set;
    }

    private static class MyEntry implements Map.Entry<Integer, Object> {
    	Integer key;
    	Object value;
    	
    	public MyEntry(Integer key, Object value) {
    		this.key = key;
    		this.value = value;
		}
    	
		@Override
		public Integer getKey() {
			return this.key;
		}

		@Override
		public Object getValue() {
			return this.value;
		}

		@Override
		public Object setValue(Object value) {
			throw new NotImplementedException();
		}
    }
    @Override
    public Set<Map.Entry<Integer, Object>> entrySet() {
    	Set<Map.Entry<Integer, Object>> set = new LinkedHashSet<>();
    	int maxColumnId = getMaxColumnId();
    	for (int i=0; i<=maxColumnId; i++) {
    		Object value = get(i);
    		if (value != null) {
    			MyEntry entry = new MyEntry(i, value);
    			set.add(entry);
    		}
    	}
    	
        return set;
    }
    
    /**
     * transaction time stamp is a unique incremental number used to version a record
     * 
     * @return
     */
    public long getTrxTimestamp() {
        return this.version;
    }
    
	public void setVersion(long version) {
		this.version = version;
	}

    /**
     * calculate header size
     * 
     * @param maxColumnId maximum possible column id
     * @return number of bytes
     */
    static int getHeaderSize(int maxColumnId) {
    	int size = OFFSET_VALUES_OFFSETS + (maxColumnId + 1) * 4;
    	return size;
    }

    public int getMaxColumnId() {
    	int value = Unsafe.getUnsignedShort(this.addr + OFFSET_MAX_COLUMN_ID);
    	return value;
    }
    
    public final static boolean isTombStone(long pRow) {
    	return pRow == 1;
	}

    public long getFieldAddress(int n) {
    	if ((n < -1) || (n > getMaxColumnId())) {
    		return 0;
    	}
    	int offset = Unsafe.getInt(this.addr + OFFSET_VALUES_OFFSETS + n * 4);
    	if (offset == 0) {
    		return 0;
    	}
    	return this.addr + offset;
    }

	@Override
	public String toString() {
		StringBuilder buf = new StringBuilder();
		buf.append("version:");
		buf.append((int)Unsafe.getByte(this.addr));
		buf.append('\n');
		buf.append("max column id:");
		buf.append(getMaxColumnId());
		buf.append('\n');
		buf.append("trx timestamp:");
		buf.append(getTrxTimestamp());
		buf.append('\n');
        buf.append("hash:");
        buf.append(UberFormatter.hex(this.getHash()));
        buf.append('\n');
		buf.append("key:");
		buf.append(KeyBytes.toString(getKeyAddress()));
		buf.append('\n');
		for (int i=0; i<=getMaxColumnId(); i++) {
			Object value = get(i);
			if (value != null) {
				buf.append(i);
				buf.append(":");
				if (value instanceof byte[]) {
					buf.append(BytesUtil.toHex((byte[])value));
				}
				else {
					buf.append(value);
				}
				buf.append('\n');
			}
		}
		buf.deleteCharAt(buf.length()-1);
		return buf.toString();
	}

	public long getAddress() {
		return this.addr;
	}

	public static long getVersion(long pRow) {
		long result = Unsafe.getLong(pRow + OFFSET_TRX_TS);
		return result;
	}

	public static void setVersion(long pRow, long version) {
		Unsafe.putLong(pRow + OFFSET_TRX_TS, version);
	}
	
	public static long getKeyAddress(long pRow) {
		int offset = Unsafe.getInt(pRow + OFFSET_KEY_OFFSET);
		return pRow + offset;
	}

	/**
	 * get the number of bytes used by the row including header
	 */
	public int getLength() {
		int size = Unsafe.getInt3(this.addr + OFFSET_LENGTH);
		return size;
	}

	public static int getLength(long pRow) {
        int size = Unsafe.getInt3(pRow + OFFSET_LENGTH);
        return size;
	}
	
	public long getVersion() {
		return this.version;
	}

	/**
	 * clone the row in the target heap
	 * @param heap
	 * @return offset in the specified heap
	 */
	public int clone(BluntHeap heap) {
		int size = getLength();
		int offset = heap.allocOffset(size);
		long addr = heap.getAddress(offset);
		Unsafe.copyMemory(getAddress(), addr, size);
		Row.setVersion(addr, getVersion());
		return offset;
	}
	
	public long getHash() {
	    long result = Unsafe.getLong(this.addr + OFFSET_HASH);
	    return result;
	}
	
	public static boolean compareByBytes(long px, long py) {
	    if (px == py) {
	        return true;
	    }
	    if ((px == 0) || (py == 0)) {
	        return false;
	    }
	    int sizeX = Row.getLength(px);
	    int sizeY = Row.getLength(py);
	    if (sizeX != sizeY) {
	        return false;
	    }
	    // we dont wanna compare tableId and version. they can be 0 after read from hbase
	    int result = Unsafe.compare(px+OFFSET_HASH, sizeX-OFFSET_HASH, py+OFFSET_HASH, sizeY-OFFSET_HASH);
	    return (result == 0) ? true : false;
	}
}
