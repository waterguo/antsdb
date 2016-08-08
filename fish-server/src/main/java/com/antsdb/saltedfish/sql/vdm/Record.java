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
package com.antsdb.saltedfish.sql.vdm;

import org.apache.commons.lang.NotImplementedException;

import com.antsdb.saltedfish.cpp.Bytes;
import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.cpp.Value;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.SlowRow;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.util.BytesUtil;

/**
 * record is not thread safe
 * 
 * @author xinyi
 *
 */
public abstract class Record {
	public final static long GROUP_END = Unsafe.allocateMemory(16);
	
    final static byte TYPE_RECORD = Value.FORMAT_RECORD;
    final static int OFFSET_SIZE = 2;
    final static int OFFSET_KEY = OFFSET_SIZE + 2;
    final static int OFFSET_FIELDS = OFFSET_KEY + 8;
    
    public abstract Object get(int field);
    public abstract Record set(int field, Object val);
    public abstract byte[] getKey();
    public abstract int size();

    static {
    	Unsafe.putByte(GROUP_END, TYPE_RECORD);
    	Unsafe.putShort(GROUP_END + OFFSET_SIZE, (short)0);
    }
    
    public Object getString(int i) {
        return (String)get(i);
    }
    
    public boolean isEmpty() {
        return getKey() == null;
    }
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append('{');
        for (int i=0; i<this.size(); i++) {
            buf.append('"');
            buf.append(i);
            buf.append('"');
            buf.append(':');
            Object value =  get(i);
            if (value instanceof String) {
                buf.append('"');
                buf.append(value);
                buf.append('"');
            }
            else {
                buf.append(value);
            }
            buf.append(',');
        }
        if (this.size() > 0) {
            buf.deleteCharAt(buf.length()-1);
        }
        buf.append('}');
        return buf.toString();
    }

    public final static long alloc(Heap heap, int nFields) {
    	if (nFields >= Short.MAX_VALUE) {
    		throw new IllegalArgumentException();
    	}
    	int bytes = OFFSET_FIELDS + nFields * 8;
    	long p = heap.alloc(bytes);
    	Unsafe.putByte(p, TYPE_RECORD);
    	Unsafe.putShort(p + OFFSET_SIZE, (short)nFields);
    	reset(p);
    	return p;
    }
    
	public static void reset(long pRecord) {
    	checkType(pRecord);
		int bytes = 8 + size(pRecord) * 8; 
    	Unsafe.setMemory(pRecord + OFFSET_KEY, bytes, (byte)0);
	}
	
    public final static long getValueAddress(long pRecord, int field) {
    	long pValue = get(pRecord, field);
    	return pValue;
    }
    
    public final static Object getValue(long pRecord, int field) {
    	long pValue = get(pRecord, field);
    	if (pValue == 0) {
    		return null;
    	}
    	Object value = FishObject.get(null, pValue);
    	return value;
    }
    
    public final static long get(long pRecord, int field) {
    	checkType(pRecord);
		int size = size(pRecord);
		if ((field < 0) || (field >=size)) {
			return 0;
		}
    	long pField = pRecord + OFFSET_FIELDS + field * 8;
    	long pValue = Unsafe.getLong(pField);
    	return pValue;
    }
    
	public final static void set(long pRecord, int field, long pValue) {
    	checkType(pRecord);
    	checkField(pRecord, field);
    	long pField = pRecord + OFFSET_FIELDS + field * 8;
    	Unsafe.putLong(pField, pValue);
    }
    
    public final static int size(long pRecord) {
    	checkType(pRecord);
    	long pSize = pRecord + OFFSET_SIZE;
    	int size = Unsafe.getShort(pSize);
    	return size;
    }
    
    public final static long getKey(long pRecord) {
    	checkType(pRecord);
    	long pKey = Unsafe.getLong(pRecord + OFFSET_KEY);
    	return pKey;
	}
    
    public final static byte[] getKeyBytes(long pRecord) {
    	long pKey = getKey(pRecord);
    	if (pKey == 0) {
    		return null;
    	}
    	return Bytes.get(null, pKey);
    }
    
    public final static void setKey(long pRecord, long pKey) {
    	checkType(pRecord);
    	Unsafe.putLong(pRecord + OFFSET_KEY, pKey);
    }
    
	public static void setKey(Heap heap, long pRecord, byte[] bytes) {
    	long pBytes = bytes != null ? Bytes.allocSet(heap, bytes) : 0;
    	setKey(pRecord, pBytes);
	}
	
    private final static void checkType(long pRecord) {
    	if (pRecord == 0) {
    		throw new IllegalArgumentException();
    	}
    	byte type = Unsafe.getByte(pRecord);
    	if (type != TYPE_RECORD) {
    		throw new IllegalArgumentException();
    	}
    }

    private final static void checkField(long pRecord, int field) {
		int size = size(pRecord);
		if ((field < 0) || (field >=size)) {
			throw new IllegalArgumentException();
		}
	}
    
	public static void set(Heap heap, long pRecord, Record rec) {
		for (int i=0; i<rec.size(); i++) {
			Object value = rec.get(i);
			long pValue = FishObject.allocSet(heap, value);
			Record.set(pRecord, i, pValue);
		}
	}
	
	public final static boolean isGroupEnd(long pRecord) {
		return pRecord == GROUP_END;
	}
	
	public static boolean isEmpty(long pRecord) {
		return getKey(pRecord) == 0;
	}
	
	public static Record toRecord(long pRecord) {
		Record rec = new HashMapRecord();
		int size = size(pRecord);
		for (int i=0; i<size; i++) {
			Object value = getValue(pRecord, i);
			rec.set(i, value);
		}
		return rec;
	}
	
	public static long fromRow(Heap heap, TableMeta table, SlowRow row) {
		throw new NotImplementedException();
	}
	
	public static long fromRow(Heap heap, TableMeta table, Row row) {
		long pRecord = alloc(heap, table.getColumns().size() + 2);
		int i=0;
		setKey(pRecord, row.getKeyAddress());
		set(pRecord, i++, row.getFieldAddress(-1));
		set(pRecord, i++, row.getFieldAddress(0));
		for (ColumnMeta column:table.getColumns()) {
			long pValue = row.getFieldAddress(column.getColumnId());
			set(pRecord, i++, pValue);
		}
		return pRecord;
	}
	
	public static String dump(long pRecord) {
		if (pRecord == 0) {
			return "NULL";
		}
		StringBuilder buf = new StringBuilder();
		try {
			buf.append("size:");
			buf.append(size(pRecord));
			buf.append("\n");
			buf.append("key:");
			buf.append(BytesUtil.toHex(getKeyBytes(pRecord)));
			buf.append("\n");
			for (int i=0; i<size(pRecord); i++) {
				buf.append(i);
				buf.append(":");
				buf.append(getValue(pRecord, i));
				buf.append("\n");
			}
		}
		catch (Exception x) {
			return "not a record";
		}
		return buf.toString();
	}
}

