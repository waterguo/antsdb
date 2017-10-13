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
import com.antsdb.saltedfish.cpp.Int4;
import com.antsdb.saltedfish.cpp.Int8;
import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.cpp.UnsafeBigEndian;
import com.antsdb.saltedfish.cpp.Value;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.VaporizingRow;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.google.common.primitives.UnsignedLongs;

/**
 * key generation. 8 bytes aligned
 * 
 * @author wgu0
 */
public final class KeyMaker {
	static KeyMaker _fullIndexKeyMaker = null;
	static KeyMaker _integerMaker;
	static KeyMaker _stringMaker;
	
	List<ColumnMeta> columns;
	List<KeyField> keyFields = new ArrayList<>();
	boolean isVariableLength = false;
	boolean isUnique;

	static {
		/*
		_rowid.setColumnName("rowid");
		_rowid.setType(DataType.longtype());
		_rowid.setColumnId(0);
		*/
		_integerMaker = new KeyMaker(new VariableLengthIntegerField(0));
		_stringMaker = new KeyMaker(new VariablLengthStringField(0));
		_fullIndexKeyMaker = new KeyMaker(
				new VariablLengthStringField(1),
				new RowidField());
	}
	
    private static interface Callback {
        long getValue(int index, KeyField field);
    }
    
	public static abstract class KeyField {
		int columnId;
		
		KeyField(int columnId) {
			this.columnId = columnId;
		}
		
		KeyField(ColumnMeta column) {
			this.columnId = column.getColumnId();
		}
		
		int getColumndId() {
			return this.columnId;
		}
		
		abstract int writeValue(Heap heap, long pTarget, long pEnd, long pValue);
		abstract int writeMaxValue(Heap heap, long pTarget, long pEnd);
		abstract int writeMinValue(Heap heap, long pTarget, long pEnd);
	}
	
	static class VariableLengthIntegerField extends KeyField {
		int length;
		
		VariableLengthIntegerField(int columnId) {
			super(columnId);
		}
		
		VariableLengthIntegerField(ColumnMeta column) {
			super(column);
		}

		@Override
		int writeValue(Heap heap, long pTarget, long pEnd, long pValue) {
			// null
			
			if (pValue == 0) {
			    ensureCapacity(pTarget, pEnd, 1);
				UnsafeBigEndian.putByte(pTarget, (byte)0);
				return -1;
			}
			
			// not null
			
			long n = FishObject.toLong(heap, pValue);
			if (n >= 0) {
				if (n <= 0xff) {
	                ensureCapacity(pTarget, pEnd, 2);
					UnsafeBigEndian.putByte(pTarget, (byte)0x81);
					UnsafeBigEndian.putByte(pTarget+1, (byte)n);
					return 2;
				}
				else if (n <= 0xffff) {
	                ensureCapacity(pTarget, pEnd, 3);
					UnsafeBigEndian.putByte(pTarget, (byte)0x82);
					UnsafeBigEndian.putShort(pTarget+1, (short)n);
					return 3;
				}
				else if (n <= 0xffffff) {
	                ensureCapacity(pTarget, pEnd, 4);
					UnsafeBigEndian.putByte(pTarget, (byte)0x83);
					UnsafeBigEndian.putInt3(pTarget+1, (int)n);
					return 4;
				}
				else if (n <= 0xffffffffl) {
	                ensureCapacity(pTarget, pEnd, 5);
					UnsafeBigEndian.putByte(pTarget, (byte)0x84);
					UnsafeBigEndian.putInt(pTarget+1, (int)n);
					return 5;
				}
				else if (n <= 0xffffffffffl) {
	                ensureCapacity(pTarget, pEnd, 6);
					UnsafeBigEndian.putByte(pTarget, (byte)0x85);
					UnsafeBigEndian.putInt5(pTarget+1, n);
					return 6;
				}
				else if (n <= 0xffffffffffffl) {
	                ensureCapacity(pTarget, pEnd, 7);
					UnsafeBigEndian.putByte(pTarget, (byte)0x86);
					UnsafeBigEndian.putInt6(pTarget+1, n);
					return 7;
				}
				else if (n <= 0xffffffffffffffl) {
	                ensureCapacity(pTarget, pEnd, 8);
					UnsafeBigEndian.putByte(pTarget, (byte)0x87);
					UnsafeBigEndian.putInt7(pTarget+1, n);
					return 8;
				}
				else {
	                ensureCapacity(pTarget, pEnd, 9);
					UnsafeBigEndian.putByte(pTarget, (byte)0x88);
					UnsafeBigEndian.putLong(pTarget + 1, n);
					return 9;
				}
			}
			else {
				if (n >= (-1l << 8)) {
	                ensureCapacity(pTarget, pEnd, 2);
					UnsafeBigEndian.putByte(pTarget, (byte)(-1 & 0x7f));
					UnsafeBigEndian.putByte(pTarget+1, (byte)n);
					return 2;
				}
				else if (n >= (-1l << 16)) {
	                ensureCapacity(pTarget, pEnd, 3);
					UnsafeBigEndian.putByte(pTarget, (byte)(-2 & 0x7f));
					UnsafeBigEndian.putShort(pTarget+1, (short)n);
					return 3;
				}
				else if (n >= (-1l << 24)) {
	                ensureCapacity(pTarget, pEnd, 4);
					UnsafeBigEndian.putByte(pTarget, (byte)(-3 & 0x7f));
					UnsafeBigEndian.putShort(pTarget+1, (short)(n >> 8));
					UnsafeBigEndian.putByte(pTarget+3, (byte)n);
					return 4;
				}
				else if (n >= (-1l << 32)) {
	                ensureCapacity(pTarget, pEnd, 5);
					UnsafeBigEndian.putByte(pTarget, (byte)(-4 & 0x7f));
					UnsafeBigEndian.putInt(pTarget+1, (int)n);
					return 5;
				}
				else if (n >= (-1l << 40)) {
	                ensureCapacity(pTarget, pEnd, 6);
					UnsafeBigEndian.putByte(pTarget, (byte)(-5 & 0x7f));
					UnsafeBigEndian.putInt(pTarget+1, (int)(n >> 8));
					UnsafeBigEndian.putByte(pTarget+5, (byte)n);
					return 6;
				}
				else if (n >= (-1l << 48)) {
	                ensureCapacity(pTarget, pEnd, 7);
					UnsafeBigEndian.putByte(pTarget, (byte)(-6 & 0x7f));
					UnsafeBigEndian.putInt(pTarget+1, (int)(n >> 16));
					UnsafeBigEndian.putShort(pTarget+5, (short)n);
					return 7;
				}
				else if (n > (-1l << 56)) {
	                ensureCapacity(pTarget, pEnd, 8);
					UnsafeBigEndian.putByte(pTarget, (byte)(-7 & 0x7f));
					UnsafeBigEndian.putInt(pTarget+1, (int)(n >> 24));
					UnsafeBigEndian.putShort(pTarget+5, (short)(n >> 8));
					UnsafeBigEndian.putByte(pTarget+7, (byte)n);
					return 8;
				}
				else {
	                ensureCapacity(pTarget, pEnd, 9);
					UnsafeBigEndian.putByte(pTarget, (byte)(-8 & 0x7f));
					UnsafeBigEndian.putLong(pTarget + 1, n);
					return 9;
				}
			}
		}

        @Override
		int writeMaxValue(Heap heap, long pTarget, long pEnd) {
            ensureCapacity(pTarget, pEnd, 1);
			UnsafeBigEndian.putByte(pTarget, (byte)0xff);
			return 1;
		}

		@Override
		int writeMinValue(Heap heap, long pTarget, long pEnd) {
            ensureCapacity(pTarget, pEnd, 1);
			UnsafeBigEndian.putByte(pTarget, (byte)0);
			return 1;
		}
	}
	
	static class RowidField extends VariableLengthIntegerField {

		RowidField() {
			super(0);
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
		int writeValue(Heap heap, long pTarget, long pEnd, long pValue) {
			if (pValue == 0) {
			    ensureCapacity(pTarget, pEnd, 8);
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
                ensureCapacity(pTarget, pEnd, 8);
				UnsafeBigEndian.putLong(pTarget, n);
				return 8;
			}
		}

		@Override
		int writeMaxValue(Heap heap, long pTarget, long pEnd) {
            ensureCapacity(pTarget, pEnd, 8);
			UnsafeBigEndian.putLong(pTarget, UnsignedLongs.MAX_VALUE);
			return 8;
		}

		@Override
		int writeMinValue(Heap heap, long pTarget, long pEnd) {
            ensureCapacity(pTarget, pEnd, 8);
			UnsafeBigEndian.putLong(pTarget, 1);
			return 0;
		}
		
	}
	
	static class TimestampField extends KeyField {

		TimestampField(ColumnMeta column) {
			super(column);
		}

		@Override
		int writeValue(Heap heap, long pTarget, long pEnd, long pValue) {
			if (pValue == 0) {
			    ensureCapacity(pTarget, pEnd, 8);
				UnsafeBigEndian.putLong(pTarget, 0);
				return -8;
			}
			else {
				long pTimestamp = FishObject.toTimestamp(heap, pValue);
				long n = FishObject.toLong(heap, pTimestamp);
                ensureCapacity(pTarget, pEnd, 8);
				UnsafeBigEndian.putLong(pTarget, n);
				return 8;
			}
		}

		@Override
		int writeMaxValue(Heap heap, long pTarget, long pEnd) {
            ensureCapacity(pTarget, pEnd, 8);
			UnsafeBigEndian.putLong(pTarget, Long.MAX_VALUE);
			return 8;
		}

		@Override
		int writeMinValue(Heap heap, long pTarget, long pEnd) {
            ensureCapacity(pTarget, pEnd, 8);
			UnsafeBigEndian.putLong(pTarget, Long.MIN_VALUE);
			return 0;
		}
		
	}
	
	static class FloatField extends KeyField {
		FloatField(ColumnMeta column) {
			super(column);
		}
		
		@Override
		int writeValue(Heap heap, long pTarget, long pEnd, long pValue) {
			if (pValue == 0) {
	            ensureCapacity(pTarget, pEnd, 8);
				UnsafeBigEndian.putFloat(pTarget, Float.NaN);
				return -8;
			}
			else {
				pValue = FishObject.toFloat(heap, pValue);
				double value = Float8.get(null, pValue);
	            ensureCapacity(pTarget, pEnd, 8);
				UnsafeBigEndian.putDouble(pTarget, value);
				return 8;
			}
		}

		@Override
		int writeMaxValue(Heap heap, long pTarget, long pEnd) {
            ensureCapacity(pTarget, pEnd, 8);
			UnsafeBigEndian.putFloat(pTarget, Float.MAX_VALUE);
			return 8;
		}

		@Override
		int writeMinValue(Heap heap, long pTarget, long pEnd) {
            ensureCapacity(pTarget, pEnd, 8);
			UnsafeBigEndian.putFloat(pTarget, Float.NEGATIVE_INFINITY);
			return 0;
		}
	}
	
    static final class VariablLengthBinaryField extends KeyField {

        VariablLengthBinaryField(ColumnMeta column) {
            super(column);
        }

        @Override
        int writeValue(Heap heap, long pTarget, long pEnd, long pValue) {
            
            // null
            
            if (pValue == 0) {
                ensureCapacity(pTarget, pEnd, 2);
                Unsafe.putShort(pTarget, (short)0);
                return -2;
            }
            
            // empty 
            
            int length = Bytes.getLength(pValue);
            if (length == 0) {
                ensureCapacity(pTarget, pEnd, 2);
                UnsafeBigEndian.putShort(pTarget, (short)1);
                return 2;
            }
            
            // normal
            
            long pStart = pTarget;
            for (int i=0; i<length; i++) {
                byte value = Bytes.get(pValue, i);
                if (value == 0xff) {
                    ensureCapacity(pTarget, pEnd, 2);
                    UnsafeBigEndian.putShort(pTarget, (short)0xfffe);
                    pTarget += 2;
                }
                else if (value == 0) {
                    ensureCapacity(pTarget, pEnd, 2);
                    UnsafeBigEndian.putShort(pTarget, (short)2);
                    pTarget += 2;
                }
                else {
                    ensureCapacity(pTarget, pEnd, 1);
                    Unsafe.putByte(pTarget, value);
                    pTarget++;
                }
            }
            
            // separator
            
            ensureCapacity(pTarget, pEnd, 2);
            Unsafe.putShort(pTarget, (short)0);
            pTarget += 2;
            
            // done
            
            return (int)(pTarget - pStart);
        }

        @Override
        int writeMaxValue(Heap heap, long pTarget, long pEnd) {
            ensureCapacity(pTarget, pEnd, 2);
            UnsafeBigEndian.putShort(pTarget, (short)0xffff);
            return 2;
        }

        @Override
        int writeMinValue(Heap heap, long pTarget, long pEnd) {
            ensureCapacity(pTarget, pEnd, 2);
            Unsafe.putShort(pTarget, (short)0);
            return 2;
        }
        
    }
    
	static final class VariablLengthStringField extends KeyField {

		/**
		 * 
		 * @param capacity number of characters
		 */
		VariablLengthStringField(int columnId) {
			super(columnId);
		}
		
		VariablLengthStringField(ColumnMeta column) {
			super(column);
		}

		@Override
		int writeValue(Heap heap, long pTarget, long pEnd, long pValue) {
			// null?
			
			if (pValue == 0) {
			    ensureCapacity(pTarget, pEnd, 1);
				Unsafe.putByte(pTarget, (byte)0);
				return -1;
			}
			
			// empty string ?
			
			pValue = FishObject.toUtf8(heap, pValue);
			int size = FishUtf8.getStringSize(Value.FORMAT_UTF8, pValue);
			if (size == 0) {
                ensureCapacity(pTarget, pEnd, 2);
				Unsafe.putShort(pTarget, (short)0);
				return 2;
			}
			
			// neither
			
            ensureCapacity(pTarget, pEnd, size + 1);
			Unsafe.copyMemory(pValue + FishUtf8.HEADER_SIZE, pTarget, size);
			Unsafe.putByte(pTarget + size, (byte)0);
			return size + 1;
		}

		@Override
		int writeMaxValue(Heap heap, long pTarget, long pEnd) {
			// first byte in utf8 cant be 0xff
            ensureCapacity(pTarget, pEnd, 1);
			UnsafeBigEndian.putByte(pTarget, (byte)0xff);
			return 1;
		}

		@Override
		int writeMinValue(Heap heap, long pTarget, long pEnd) {
            ensureCapacity(pTarget, pEnd, 1);
			UnsafeBigEndian.putByte(pTarget, (byte)0);
			return 1;
		}
	}
	
    private KeyMaker(KeyField field) {
    	this.isUnique = true;
    	this.keyFields.add(field);
    }
    
    private KeyMaker(KeyField ... fields) {
    	this.isUnique = false;
    	for (KeyField field:fields) {
    		this.keyFields.add(field);
    	}
    }
    
    public KeyMaker(List<ColumnMeta> columns, boolean isUnique) {
    	this.columns = columns;
    	this.isUnique = isUnique;
    	if (columns.size() == 0) {
    		KeyField field = new RowidField();
    		this.keyFields.add(field);
    		return;
    	}
    	for (ColumnMeta i:columns) {
    		KeyField field = createKeyField(i);
    		this.keyFields.add(field);
    	}
    	
    	// append rowid if the index is not unique
    	
		if (!isUnique) {
			KeyField field = new RowidField();
    		this.keyFields.add(field);
		}
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
			return new VariablLengthBinaryField(i);
		}
		else {
			throw new NotImplementedException();
		}
	}
	
	public long make(Heap heap, int nFields, long pRowid, boolean fillMax, boolean fillMin, Callback callback) {
		KeyBytes key = KeyBytes.alloc(heap, 0x1000);
		long pStart = key.getAddress() + 4;
		long pEnd = pStart + 0x1000;
		long p = pStart;
		boolean hasNull = false;
		
		// key fields
		
		int posRowid = 0;
		int sizeRowid = 0;
		for (int i=0; i<nFields; i++) {
			KeyField ii = this.keyFields.get(i);
			long pValue = callback.getValue(i, ii);
			if (pValue == 1) {
			    // value is null and caller doesnt want key
			    return 0;
			}
			int nBytes = ii.writeValue(heap, p, pEnd, pValue);
			if (nBytes == 0) {
				// callback wants to exit
				return 0;
			}
			if (!this.isUnique && (i == nFields-1) && (ii instanceof RowidField)) {
				posRowid = (int)(p - pStart);
				sizeRowid = nBytes;
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
						nBytes = ii.writeMaxValue(heap, p, pEnd);
					}
					else if (fillMin) {
						nBytes = ii.writeMinValue(heap, p, pEnd);
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
		
		int length = (int)(p - pStart);
		int mod = length % 8;
		if (mod != 0) {
			int delta = 8 - mod;
			Unsafe.setMemory(p, delta, (byte)0);
			length += delta;
		}
		
		// flip to little endian
		
		flipEndian(key.getAddress() + 4, length);

		// tell the keybytes position of rowid suffix
		
		key.resize(length);
		if (!this.isUnique) {
			key.setSuffixPostion(posRowid, sizeRowid);
		}
		
		// done
		
		return key.getAddress();
	}
	
	public long make(Heap heap, long ... values) {
		long pResult = make(heap, values.length, 0, false, true, (int i, KeyField field) -> {
			long pValue = values[i];
			return pValue;
		});
		return pResult;
	}
	
	public long makeMax(Heap heap, long ... values) {
		long pResult = make(heap, values.length, 0, true, false, (int i, KeyField field) -> {
			long pValue = values[i];
			return pValue;
		});
		return pResult;
	}
	
	public long make(VdmContext ctx, Heap heap, List<Operator> exprs, Parameters params, long pRecord) {
	    Callback callback = (int i, KeyField field) -> {
            Operator expr = exprs.get(i);
            long pValue = expr.eval(ctx, heap, params, pRecord);
            return pValue;
        };
		long pResult = make(heap, this.keyFields.size(), 0, false, false, callback);
		return pResult;
	}
	
	public long makeMax(
			VdmContext ctx, 
			Heap heap, 
			List<Operator> exprs, 
			Parameters params, 
			long pRecord, 
			boolean isNullable) {
		long pResult = make(heap, exprs.size(), 0, true, false, (int i, KeyField field) -> {
			Operator expr = exprs.get(i);
			long pValue = expr.eval(ctx, heap, params, pRecord);
			if ((pValue == 0) && !isNullable) {
				// caller doesnt want to generate value for null valued field
				return 1;
			}
			return pValue;
		});
		return pResult;
	}
	
	public long makeMin(VdmContext ctx, 
	                    Heap heap, 
	                    List<Operator> exprs, 
	                    Parameters params, 
	                    long pRecord, 
	                    boolean isNullable) {
		long pResult = make(heap, exprs.size(), 0, false, true, (int i, KeyField field) -> {
			Operator expr = exprs.get(i);
			long pValue = expr.eval(ctx, heap, params, pRecord);
			if ((pValue == 0) && !isNullable) {
				// caller doesnt want to generate value for null valued field
				return 1;
			}
			return pValue;
		});
		return pResult;
	}
	
	public long make(Heap heap, Row row) {
		long pRowid = row.getFieldAddress(0);
		long pResult;
		Callback callback = (int i, KeyField field) -> {
            long pValue = row.getFieldAddress(field.getColumndId());
            return pValue;
        };
		pResult = make(heap, this.keyFields.size(), pRowid, false, false, callback);
		return pResult;
	}
	
	public long make(Heap heap, VaporizingRow row) {
		long pRowid = row.getFieldAddress(0);
		long pResult;
		pResult = make(heap, this.keyFields.size(), pRowid, false, false, (int i, KeyField field) -> {
			long pValue = row.getFieldAddress(field.getColumndId());
			return pValue;
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

	public static byte[] gen(Object... values) {
        try (BluntHeap heap = new BluntHeap()) {
            long[] pValues = new long[values.length];
            KeyField[] fields = new KeyField[values.length];
            for (int i=0; i<values.length; i++) {
                Object value = values[i];
                long pValue;
                KeyField field;
                if (value instanceof Integer) {
                    pValue = Int4.allocSet(heap, (Integer)value);
                    field = new VariableLengthIntegerField(i);
                }
                else if (value instanceof Long) {
                    pValue = Int8.allocSet(heap, (Long)value);
                    field = new VariableLengthIntegerField(i);
                }
                else if (value instanceof String) {
                    pValue = FishUtf8.allocSet(heap, (String)value);
                    field = new VariablLengthStringField(i);
                }
                else {
                    throw new IllegalArgumentException();
                }
                pValues[i] = pValue;
                fields[i] = field;
            }
            long pKey = new KeyMaker(fields).make(heap, pValues);
            KeyBytes key = new KeyBytes(pKey);
            return key.get();
        }
	}
	
	public static byte[] make(long value) {
		try (BluntHeap heap = new BluntHeap()) {
			long pValue = Int8.allocSet(heap, value);
			long[] values = new long[] {pValue};
			long pKey = _integerMaker.make(heap, values);
			KeyBytes key = new KeyBytes(pKey);
			return key.get();
		}
	}

	public static byte[] make(String value) {
		try (BluntHeap heap = new BluntHeap()) {
			long pValue = FishUtf8.allocSet(heap, value);
			long[] values = new long[] {pValue};
			long pKey = _stringMaker.make(heap, values);
			KeyBytes key = new KeyBytes(pKey);
			return key.get();
		}
	}
	
    public static long make_(Heap heap, long value) {
        long pValue = Int8.allocSet(heap, value);
        long[] values = new long[] {pValue};
        long pResult = _integerMaker.make(heap, values);
        return pResult;
    }
    
	public static long make(Heap heap, String value) {
		long pValue = FishUtf8.allocSet(heap, value);
		long[] values = new long[] {pValue};
		long pKey = _stringMaker.make(heap, values);
		return pKey;
	}	
	
	public static KeyMaker getSingleStringMaker() {
		return _stringMaker;
	}
	
	public static KeyMaker getFullTextIndexKeyMaker() {
		return _fullIndexKeyMaker;
	}

    private static void ensureCapacity(long pTarget, long pEnd, int size) {
        int capacity = (int)(pEnd - pTarget);
        if (capacity < size) {
            throw new IllegalArgumentException("key is too big: " + (capacity + size));
        }
    }

}
