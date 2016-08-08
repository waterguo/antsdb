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
package com.antsdb.saltedfish.util;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;

import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.vdm.Cursor;
import com.antsdb.saltedfish.sql.vdm.FieldMeta;
import com.antsdb.saltedfish.sql.vdm.CursorMeta;
import com.antsdb.saltedfish.sql.vdm.CursorWithHeap;
import com.antsdb.saltedfish.sql.vdm.HashMapRecord;
import com.antsdb.saltedfish.sql.vdm.Marker;
import com.antsdb.saltedfish.sql.vdm.Record;

public class CursorUtil {

	static class IteratorCursor extends CursorWithHeap {
		Iterator<?> iter;
		
		public IteratorCursor(CursorMeta meta, Iterator<?> iter) {
			super(meta);
			this.iter = iter;
		}

		@Override
		public long next() {
			try {
				if (this.getHeap() == null) {
					return 0;
				}
				if (!iter.hasNext()) {
					return 0;
				}
				Object obj = iter.next();
		    	if (Marker.GROUP_END == obj) {
		    		return Record.GROUP_END;
		    	}
				Heap heap = newHeap();
				long pRecord = newRecord();
				toRecord(heap, getMetadata(), pRecord, obj);
				return pRecord;
			}
			catch (Exception x) {
				throw new OrcaException(x);
			}
		}

		@Override
		public void close() {
			super.close();
		}

	}
	
	static class SingleRecordCursor extends Cursor {
		long pRecord;
		
		public SingleRecordCursor(CursorMeta meta, long pRecord) {
			super(meta);
			this.pRecord = pRecord;
		}

		@Override
		public long next() {
			long pResult = this.pRecord;
			this.pRecord = 0;
			return pResult;
		}

		@Override
		public void close() {
		}

	}
	
    public static List<Record> readAll(Cursor c) {
        List<Record> rows = new ArrayList<>();
        for (long pRecord = c.next(); pRecord != 0; pRecord = c.next()) {
        	HashMapRecord rec = new HashMapRecord();
        	for (int j=0; j<Record.size(pRecord); j++) {
        		Object value = FishObject.get(null, Record.get(pRecord, j));
        		rec.set(j, value);
        	}
            rows.add(rec);
        }
        c.close();
        return rows;
    }
    
    public static void toRecord(Heap heap, CursorMeta meta, long pRecord, Object obj) 
	throws Exception {
    	if (obj instanceof Record) {
    		Record rec = (Record)obj;
    		byte[] key = rec.getKey();
    		Record.setKey(heap, pRecord, key);
    		for (int field = 0; field < meta.getColumns().size(); field++) {
				Object value = rec.get(field);
				long pValue = FishObject.allocSet(heap, value);
				Record.set(pRecord, field, pValue);
    		}
    	}
    	else {
    		int field = 0;
    		for (FieldMeta i:meta.getColumns()) {
				Field f;
				f = obj.getClass().getField(i.getName());
				Object value = f.get(obj);
				long pValue = FishObject.allocSet(heap, value);
				Record.set(pRecord, field, pValue);
    	    	field++;
    		}
    	}
	}

	public static CursorMeta toMeta(Class<?> klass) {
    	CursorMeta meta = new CursorMeta();
        for (Field i:klass.getFields()) {
        	FieldMeta column = new FieldMeta();
            column.setName(i.getName());
            if (i.getType() == String.class) {
                column.setType(DataType.varchar());
            }
            else if (i.getType() == int.class) {
                column.setType(DataType.integer());
            }
            else if (i.getType() == Integer.class) {
                column.setType(DataType.integer());
            }
            else if (i.getType() == long.class) {
                column.setType(DataType.longtype());
            }
            else {
                throw new NotImplementedException();
            }
            meta.addColumn(column);
        }
    	return meta;
    }

    public static Record toRecord(Object obj) throws IllegalArgumentException, IllegalAccessException {
    	HashMapRecord rec = new HashMapRecord();
    	Class<?> klass = obj.getClass();
        for (Field i:klass.getFields()) {
        	Object value = i.get(obj);
        	rec.set(rec.size(), value);
        }
    	return rec;
    }

	public static Cursor toCursor(CursorMeta meta, Iterable<?> it) {
		Cursor c = new IteratorCursor(meta, it.iterator());
		return c;
	}
	
	public static Cursor toCursor(CursorMeta meta, long pRecord) {
		Cursor c = new SingleRecordCursor(meta, pRecord);
		return c;
	}

	public static Cursor toCursor(CursorMeta meta, Record record) {
		return toCursor(meta, Collections.singletonList(record));
	}
}
