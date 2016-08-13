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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;

import com.antsdb.saltedfish.cpp.BluntHeap;
import com.antsdb.saltedfish.cpp.Bytes;
import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.FishUtf8;
import com.antsdb.saltedfish.cpp.Float8;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.Int8;
import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.cpp.UnsafeBigEndian;
import com.antsdb.saltedfish.cpp.Value;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.VaporizingRow;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.util.CodingError;
import com.google.common.primitives.UnsignedLongs;

/**
 * key generation. 8 bytes aligned
 * 
 * @author wgu0
 */
public final class KeyMaker {
	static KeyMaker _integerMaker;
	static KeyMaker _stringMaker;
	
	List<ColumnMeta> columns;
	List<KeyField> keyFields = new ArrayList<>();
	int  size = 0;
	boolean isVariableLength = false;
	boolean isUnique;

	static {
		/*
		_rowid.setColumnName("rowid");
		_rowid.setType(DataType.longtype());
		_rowid.setColumnId(0);
		*/
		_integerMaker = new KeyMaker(new VariableLengthIntegerField(null), 9);
		_stringMaker = new KeyMaker(new VariablLengthStringField(null), 100);
	}
	
	private static interface Callback {
		int writeValue(long pTarget, int index, KeyField field);
	}
	
	static abstract class KeyField {
		ColumnMeta column;
		
		KeyField(ColumnMeta column) {
			this.column = column;
		}
		
		int getColumndId() {
			return this.column.getColumnId();
		}
		
		abstract boolean isVariableLength();
		abstract int getLength();
		abstract int writeValue(Heap heap, long pTarget, long pValue);
		abstract int writeMaxValue(Heap heap, long pTarget);
		abstract int writeMinValue(Heap heap, long pTarget);
	}
	
	static class VariableLengthIntegerField extends KeyField {
		
		VariableLengthIntegerField(ColumnMeta column) {
			super(column);
		}

		@Override
		boolean isVariableLength() {
			return true;
		}

		@Override
		int getLength() {
			return 9;
		}

		@Override
		int writeValue(Heap heap, long pTarget, long pValue) {
			// null
			
			if (pValue == 0) {
				UnsafeBigEndian.putByte(pTarget, (byte)0);
				return -1;
			}
			
			// not null
			
			long n = FishObject.toLong(heap, pValue);
			if (n >= 0) {
				if (n <= 0xff) {
					UnsafeBigEndian.putByte(pTarget, (byte)0x81);
					UnsafeBigEndian.putByte(pTarget+1, (byte)n);
					return 2;
				}
				else if (n <= 0xffff) {
					UnsafeBigEndian.putByte(pTarget, (byte)0x82);
					UnsafeBigEndian.putShort(pTarget+1, (short)n);
					return 3;
				}
				else if (n <= 0xffffff) {
					UnsafeBigEndian.putByte(pTarget, (byte)0x83);
					UnsafeBigEndian.putInt3(pTarget+1, (int)n);
					return 4;
				}
				else if (n <= 0xffffffffl) {
					UnsafeBigEndian.putByte(pTarget, (byte)0x84);
					UnsafeBigEndian.putInt(pTarget+1, (int)n);
					return 5;
				}
				else if (n <= 0xffffffffffl) {
					UnsafeBigEndian.putByte(pTarget, (byte)0x85);
					UnsafeBigEndian.putInt5(pTarget+1, n);
					return 6;
				}
				else if (n <= 0xffffffffffffl) {
					UnsafeBigEndian.putByte(pTarget, (byte)0x86);
					UnsafeBigEndian.putInt6(pTarget+1, n);
					return 7;
				}
				else if (n <= 0xffffffffffffffl) {
					UnsafeBigEndian.putByte(pTarget, (byte)0x87);
					UnsafeBigEndian.putInt7(pTarget+1, n);
					return 8;
				}
				else {
					UnsafeBigEndian.putByte(pTarget, (byte)0x88);
					UnsafeBigEndian.putLong(pTarget + 1, n);
					return 9;
				}
			}
			else {
				if (n >= (-1l << 8)) {
					UnsafeBigEndian.putByte(pTarget, (byte)(-1 & 0x7f));
					UnsafeBigEndian.putByte(pTarget+1, (byte)n);
					return 2;
				}
				else if (n >= (-1l << 16)) {
					UnsafeBigEndian.putByte(pTarget, (byte)(-2 & 0x7f));
					UnsafeBigEndian.putShort(pTarget+1, (short)n);
					return 3;
				}
				else if (n >= (-1l << 24)) {
					UnsafeBigEndian.putByte(pTarget, (byte)(-3 & 0x7f));
					UnsafeBigEndian.putShort(pTarget+1, (short)(n >> 8));
					UnsafeBigEndian.putByte(pTarget+3, (byte)n);
					return 4;
				}
				else if (n >= (-1l << 32)) {
					UnsafeBigEndian.putByte(pTarget, (byte)(-4 & 0x7f));
					UnsafeBigEndian.putInt(pTarget+1, (int)n);
					return 5;
				}
				else if (n >= (-1l << 40)) {
					UnsafeBigEndian.putByte(pTarget, (byte)(-5 & 0x7f));
					UnsafeBigEndian.putInt(pTarget+1, (int)(n >> 8));
					UnsafeBigEndian.putByte(pTarget+5, (byte)n);
					return 6;
				}
				else if (n >= (-1l << 48)) {
					UnsafeBigEndian.putByte(pTarget, (byte)(-6 & 0x7f));
					UnsafeBigEndian.putInt(pTarget+1, (int)(n >> 16));
					UnsafeBigEndian.putShort(pTarget+5, (short)n);
					return 7;
				}
				else if (n > (-1l << 56)) {
					UnsafeBigEndian.putByte(pTarget, (byte)(-7 & 0x7f));
					UnsafeBigEndian.putInt(pTarget+1, (int)(n >> 24));
					UnsafeBigEndian.putShort(pTarget+5, (short)(n >> 8));
					UnsafeBigEndian.putByte(pTarget+7, (byte)n);
					return 8;
				}
				else {
					UnsafeBigEndian.putByte(pTarget, (byte)(-8 & 0x7f));
					UnsafeBigEndian.putLong(pTarget + 1, n);
					return 9;
				}
			}
		}

		@Override
		int writeMaxValue(Heap heap, long pTarget) {
			UnsafeBigEndian.putByte(pTarget, (byte)0xff);
			return 1;
		}

		@Override
		int writeMinValue(Heap heap, long pTarget) {
			UnsafeBigEndian.putByte(pTarget, (byte)0);
			return 1;
		}
	}
	
	static class RowidField extends VariableLengthIntegerField {

		RowidField() {
			super(null);
		}

		@Override
		int getColumndId() {
			return 0;
		}
	}
	
	static class DateField extends KeyField {

		DateField(ColumnMeta column) {
			super(column);
		}

		@Override
		boolean isVariableLength() {
			return false;
		}

		@Override
		int getLength() {
			return 8;
		}

		@Override
		int writeValue(Heap heap, long pTarget, long pValue) {
			if (pValue == 0) {
				UnsafeBigEndian.putLong(pTarget, 0);
				return -8;
			}
			else {
				long pDate = FishObject.toDate(heap, pValue);
				long n = FishObject.toLong(heap, pDate);
				if (n == 0) {
					// minimum value allowed for timestamp is 1
					throw new IllegalArgumentException("Date column cannot be 0");
				}
				UnsafeBigEndian.putLong(pTarget, n);
				return 8;
			}
		}

		@Override
		int writeMaxValue(Heap heap, long pTarget) {
			UnsafeBigEndian.putLong(pTarget, UnsignedLongs.MAX_VALUE);
			return 8;
		}

		@Override
		int writeMinValue(Heap heap, long pTarget) {
			UnsafeBigEndian.putLong(pTarget, 1);
			return 0;
		}
		
	}
	
	static class TimestampField extends KeyField {

		TimestampField(ColumnMeta column) {
			super(column);
		}

		@Override
		boolean isVariableLength() {
			return false;
		}

		@Override
		int getLength() {
			return 8;
		}

		@Override
		int writeValue(Heap heap, long pTarget, long pValue) {
			if (pValue == 0) {
				UnsafeBigEndian.putLong(pTarget, 0);
				return -8;
			}
			else {
				long pTimestamp = FishObject.toTimestamp(heap, pValue);
				long n = FishObject.toLong(heap, pTimestamp);
				UnsafeBigEndian.putLong(pTarget, n);
				return 8;
			}
		}

		@Override
		int writeMaxValue(Heap heap, long pTarget) {
			UnsafeBigEndian.putLong(pTarget, Long.MAX_VALUE);
			return 8;
		}

		@Override
		int writeMinValue(Heap heap, long pTarget) {
			UnsafeBigEndian.putLong(pTarget, Long.MIN_VALUE);
			return 0;
		}
		
	}
	
	static class FloatField extends KeyField {
		FloatField(ColumnMeta column) {
			super(column);
		}
		
		@Override
		boolean isVariableLength() {
			return false;
		}

		@Override
		int getLength() {
			return 8;
		}

		@Override
		int writeValue(Heap heap, long pTarget, long pValue) {
			if (pValue == 0) {
				UnsafeBigEndian.putFloat(pTarget, Float.NaN);
				return -8;
			}
			else {
				pValue = FishObject.toFloat(heap, pValue);
				double value = Float8.get(null, pValue);
				UnsafeBigEndian.putDouble(pTarget, value);
				return 8;
			}
		}

		@Override
		int writeMaxValue(Heap heap, long pTarget) {
			UnsafeBigEndian.putFloat(pTarget, Float.MAX_VALUE);
			return 8;
		}

		@Override
		int writeMinValue(Heap heap, long pTarget) {
			UnsafeBigEndian.putFloat(pTarget, Float.NEGATIVE_INFINITY);
			return 0;
		}
	}
	
	static final class VariablLengthStringField extends KeyField {

		VariablLengthStringField(ColumnMeta column) {
			super(column);
		}

		@Override
		boolean isVariableLength() {
			return true;
		}

		@Override
		int getLength() {
			// assume we cover 3 bytes utf at most
			return this.column.getTypeLength() * 3 + 1;
		}

		@Override
		int writeValue(Heap heap, long pTarget, long pValue) {
			// null?
			
			if (pValue == 0) {
				Unsafe.putByte(pTarget, (byte)0);
				return -1;
			}
			
			// empty string ?
			
			pValue = FishObject.toUtf8(heap, pValue);
			int size = FishUtf8.getStringSize(Value.FORMAT_UTF8, pValue);
			if (size == 0) {
				Unsafe.putShort(pTarget, (short)0);
				return 2;
			}
			
			// neither
			
			Unsafe.copyMemory(pValue + FishUtf8.HEADER_SIZE, pTarget, size);
			Unsafe.putByte(pTarget + size, (byte)0);
			return size + 1;
		}

		@Override
		int writeMaxValue(Heap heap, long pTarget) {
			// first byte in utf8 cant be 0xff
			UnsafeBigEndian.putByte(pTarget, (byte)0xff);
			return 1;
		}

		@Override
		int writeMinValue(Heap heap, long pTarget) {
			UnsafeBigEndian.putByte(pTarget, (byte)0);
			return 1;
		}
	}
	
    private KeyMaker(KeyField field, int  size) {
    	this.isUnique = true;
    	this.keyFields.add(field);
    	this.size = size;
    }
    
    public KeyMaker(List<ColumnMeta> columns, boolean isUnique) {
    	this.columns = columns;
    	this.isUnique = isUnique;
    	if (columns.size() == 0) {
    		KeyField field = new RowidField();
    		this.keyFields.add(field);
    		this.size += field.getLength();
    		this.isVariableLength = field.isVariableLength();
    		return;
    	}
    	for (ColumnMeta i:columns) {
    		KeyField field = createKeyField(i);
    		this.keyFields.add(field);
    		this.size += field.getLength();
    		this.isVariableLength = this.isVariableLength | field.isVariableLength();
    	}
    	
    	// append rowid if the index is not unique
    	
		if (!isUnique) {
			KeyField field = new RowidField();
    		this.keyFields.add(field);
    		this.size += field.getLength();
    		this.isVariableLength = this.isVariableLength | field.isVariableLength();
		}
    	
    	// size is 8 bytes aligned
    	
    	size = ((size - 1) / 8 + 1) * 8;
    }

	private KeyField createKeyField(ColumnMeta i) {
		DataType type = i.getDataType();
		if (type.getJavaType() == Integer.class) {
			return new VariableLengthIntegerField(i);
		}
		else if (type.getJavaType() == Long.class) {
			return new VariableLengthIntegerField(i);
		}
		else if (type.getJavaType() == String.class) {
			return new VariablLengthStringField(i);
		}
		else if (type.getJavaType() == BigDecimal.class) {
			if ((type.getScale() != 0) || (type.getLength() > 19)) {
				throw new NotImplementedException();
			}
			return new VariableLengthIntegerField(i);
		}
		else if (type.getJavaType() == Date.class) {
			return new DateField(i);
		}
		else if (type.getJavaType() == Timestamp.class) {
			return new TimestampField(i);
		}
		else if (type.getJavaType() == Float.class) {
			return new FloatField(i);
		}
		else if (type.getJavaType() == Double.class) {
			return new FloatField(i);
		}
		else if (type.getJavaType() == byte[].class) {
			return new VariablLengthStringField(i);
		}
		else {
			throw new NotImplementedException();
		}
	}
	
	public long make(Heap heap, int nFields, long pRowid, boolean fillMax, boolean fillMin, Callback callback) {
		int allocSize = this.size + 4 + 8;
		long p = heap.alloc(allocSize);
		long pResult = p;
		p += 4;
		boolean hasNull = false;
		
		// key fields
		
		for (int i=0; i<nFields; i++) {
			KeyField ii = this.keyFields.get(i);
			int nBytes = callback.writeValue(p, i, ii);
			if (nBytes == 0) {
				// callback wants to exit
				return 0;
			}
			if (nBytes < 0) {
				// meaning it is null
				hasNull = true;
				nBytes = -nBytes;
			}
			p += nBytes;
		}
		
		// fillers
		
		if (nFields < this.keyFields.size()) {
			if (fillMax || fillMin) {
				for (int i=nFields; i<this.keyFields.size(); i++) {
					KeyField ii = this.keyFields.get(i);
					int nBytes = 0;
					if (fillMax) {
						nBytes = ii.writeMaxValue(heap, p);
					}
					else if (fillMin) {
						nBytes = ii.writeMinValue(heap, p);
					}
					p += nBytes;
				}
			}
		}
		else {
			// append rowid if an unique index has null value on one of its fields
			
			if (this.isUnique && hasNull) {
				// 0 means this is part of expression, it is generating the key value for a row. append rowid
				if (pRowid != 0) {
					long rowid = FishObject.toLong(heap, pRowid);
					UnsafeBigEndian.putLong(p, rowid);
					p += 8;
				}
				// for max search range, considering we have appended rowid, we need to pad an additional 0xff 
				// at the end
				if (fillMax) {
					UnsafeBigEndian.putByte(p, (byte)0xff);
					p += 1;
				}
			}
		}
		
		// 8 bytes alignment
		
		Unsafe.putByte(pResult, Value.FORMAT_BYTES);
		int length = (int)(p - pResult - 4);
		int mod = length % 8;
		if (mod != 0) {
			int delta = 8 - mod;
			Unsafe.setMemory(p, delta, (byte)0);
			length += delta;
		}
		Unsafe.putInt3(pResult + 1, length);
		if (length + 4 > allocSize) {
			throw new CodingError();
		}
		
		// flip to little endian
		
		flipEndian(pResult + 4, this.size);
		
		return pResult;
	}
	
	public long make(Heap heap, long[] values) {
		long pResult = make(heap, values.length, 0, false, true, (long pTarget, int i, KeyField field) -> {
			long pValue = values[i];
			int nBytes = field.writeValue(heap, pTarget, pValue);
			return nBytes;
		});
		return pResult;
	}
	
	public long makeMax(Heap heap, long[] values) {
		long pResult = make(heap, values.length, 0, true, false, (long pTarget, int i, KeyField field) -> {
			long pValue = values[i];
			int nBytes = field.writeValue(heap, pTarget, pValue);
			return nBytes;
		});
		return pResult;
	}
	
	public long make(VdmContext ctx, Heap heap, List<Operator> exprs, Parameters params, long pRecord) {
		long pResult = make(heap, this.keyFields.size(), 0, false, false, (long pTarget, int i, KeyField field) -> {
			Operator expr = exprs.get(i);
			long pValue = expr.eval(ctx, heap, params, pRecord);
			int nBytes = field.writeValue(heap, pTarget, pValue);
			return nBytes;
		});
		return pResult;
	}
	
	public long makeMax(
			VdmContext ctx, 
			Heap heap, 
			List<Operator> exprs, 
			Parameters params, 
			long pRecord, 
			boolean isNullable) {
		long pResult = make(heap, exprs.size(), 0, true, false, (long pTarget, int i, KeyField field) -> {
			Operator expr = exprs.get(i);
			long pValue = expr.eval(ctx, heap, params, pRecord);
			if ((pValue == 0) && !isNullable) {
				// caller doesnt want to generate value for null valued field
				return 0;
			}
			int nBytes = field.writeValue(heap, pTarget, pValue);
			return nBytes;
		});
		return pResult;
	}
	
	public long makeMin(VdmContext ctx, Heap heap, List<Operator> exprs, Parameters params, long pRecord, boolean isNullable) {
		long pResult = make(heap, exprs.size(), 0, false, true, (long pTarget, int i, KeyField field) -> {
			Operator expr = exprs.get(i);
			long pValue = expr.eval(ctx, heap, params, pRecord);
			if ((pValue == 0) && !isNullable) {
				// caller doesnt want to generate value for null valued field
				return 0;
			}
			int nBytes = field.writeValue(heap, pTarget, pValue);
			return nBytes;
		});
		return pResult;
	}
	
	public long make(Heap heap, Row row) {
		long pRowid = row.getFieldAddress(0);
		long pResult;
		pResult = make(heap, this.keyFields.size(), pRowid, false, false, (long pTarget, int i, KeyField field) -> {
			long pValue = row.getFieldAddress(field.getColumndId());
			int nBytes = field.writeValue(heap, pTarget, pValue);
			return nBytes;
		});
		return pResult;
	}
	
	public long make(Heap heap, VaporizingRow row) {
		long pRowid = row.getFieldAddress(0);
		long pResult;
		pResult = make(heap, this.keyFields.size(), pRowid, false, false, (long pTarget, int i, KeyField field) -> {
			long pValue = row.getFieldAddress(field.getColumndId());
			int nBytes = field.writeValue(heap, pTarget, pValue);
			return nBytes;
		});
		return pResult;
	}
	
	public static void flipEndian(long p, int size) {
		for (int i=0; i<size/8; i++) {
			long pValue = p + i * 8;
			long value = Unsafe.getLong(pValue);
			Unsafe.putByte(pValue + 0, (byte)(value >> 56));
			Unsafe.putByte(pValue + 1, (byte)(value >> 48));
			Unsafe.putByte(pValue + 2, (byte)(value >> 40));
			Unsafe.putByte(pValue + 3, (byte)(value >> 32));
			Unsafe.putByte(pValue + 4, (byte)(value >> 24));
			Unsafe.putByte(pValue + 5, (byte)(value >> 16));
			Unsafe.putByte(pValue + 6, (byte)(value >> 8));
			Unsafe.putByte(pValue + 7, (byte)(value));
		}
	}
	
	public static void flipEndian(byte[] bytes) {
		for (int i=0; i<bytes.length/8; i++) {
			for (int j=0; j<4; j++) {
				byte bt = bytes[i * 8 + j];
				bytes[i * 8 + j] = bytes[i * 8 + 8 - j - 1];
				bytes[i * 8 + 8 - j - 1] = bt;
			}
		}
	}

	public int getLength() {
		return this.size;
	}

	public static boolean equals(long pX, long pY) {
		if (pX == pY) {
			return true;
		}
		int xHeader = Unsafe.getInt(pX);
		int yHeader = Unsafe.getInt(pY);
		if (xHeader != yHeader) {
			return false;
		}
		int length = xHeader >>> 8;
		for (int i=0; i<=((length-1)/8); i++) {
			long x = Unsafe.getLong(pX + 4 + i*8);
			long y = Unsafe.getLong(pY + 4 + i*8);
			if (x != y) {
				return false;
			}
		}
		return true;
	}
	
	public static byte[] make(long value) {
		try (BluntHeap heap = new BluntHeap()) {
			long pValue = Int8.allocSet(heap, value);
			long[] values = new long[] {pValue};
			long pKey = _integerMaker.make(heap, values);
			return Bytes.get(heap, pKey);
		}
	}

	public static byte[] make(String value) {
		try (BluntHeap heap = new BluntHeap()) {
			long pValue = FishUtf8.allocSet(heap, value);
			long[] values = new long[] {pValue};
			long pKey = _stringMaker.make(heap, values);
			return Bytes.get(heap, pKey);
		}
	}
}
