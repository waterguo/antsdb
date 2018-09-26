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
package com.antsdb.saltedfish.util;

import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;

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
import com.antsdb.saltedfish.sql.vdm.ObjectName;
import com.antsdb.saltedfish.sql.vdm.Record;

public class CursorUtil {

    static class PropertiesCursor extends CursorWithHeap {
        private Iterator<Entry<String, Object>> iter;

        public PropertiesCursor(CursorMeta meta, Map<String, Object> props) {
            super(meta);
            this.iter = props.entrySet().iterator();
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
                Entry<String, Object> obj = iter.next();
                String name = (String) obj.getKey();
                String value = String.valueOf(obj.getValue());
                Heap heap = getHeap();
                long pRecord = newRecord();
                Record.set(pRecord, 0, FishObject.allocSet(heap, name));
                Record.set(pRecord, 1, FishObject.allocSet(heap, value));
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
                Heap heap = getHeap();
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
            for (int j = 0; j < Record.size(pRecord); j++) {
                Object value = FishObject.get(null, Record.get(pRecord, j));
                rec.set(j, value);
            }
            rows.add(rec);
        }
        c.close();
        return rows;
    }

    public static void toRecord(Heap heap, CursorMeta meta, long pRecord, Object obj) throws Exception {
        if (obj instanceof Record) {
            Record rec = (Record) obj;
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
            for (int i=0; i<meta.getColumns().size(); i++) {
                Field f = obj.getClass().getFields()[i];
                Object value = f.get(obj);
                long pValue = FishObject.allocSet(heap, value);
                Record.set(pRecord, field, pValue);
                field++;
            }
        }
    }

    public static CursorMeta toMeta(Class<?> klass) {
        return toMeta(klass, "", "");
    }
    
    public static CursorMeta toMeta(Class<?> klass, String catalog, String table) {
        CursorMeta meta = new CursorMeta();
        if (klass == Properties.class) {
            FieldMeta column = new FieldMeta();
            column.setSourceTable(new ObjectName(catalog, table));
            column.setName("name");
            column.setType(DataType.varchar());
            meta.addColumn(column);
            column = new FieldMeta();
            column.setName("value");
            column.setType(DataType.varchar());
            meta.addColumn(column);
            return meta;
        }
        for (Field i : klass.getFields()) {
            FieldMeta column = new FieldMeta();
            column.setSourceTable(new ObjectName(catalog, table));
            column.setTableAlias(table);
            column.setName(getAlias(i));
            column.setSourceColumnName(getSourceName(i));
            column.setType(getType(i));
            meta.addColumn(column);
        }
        return meta;
    }

    private static String getSourceName(Field field) {
        if (field.isAnnotationPresent(MysqlColumnMeta.class)) {
            MysqlColumnMeta ann = field.getAnnotation(MysqlColumnMeta.class);
            if (!StringUtils.isEmpty(ann.column())) {
                return ann.column();
            }
        }
        return getAlias(field);
    }

    private static String getAlias(Field field) {
        if (field.isAnnotationPresent(MysqlColumnMeta.class)) {
            MysqlColumnMeta ann = field.getAnnotation(MysqlColumnMeta.class);
            if (!StringUtils.isEmpty(ann.alias())) {
                return ann.alias();
            }
        }
        return field.getName();
    }

    private static DataType getType(Field field) {
        DataType result;
        if (field.getType() == String.class) {
            result = DataType.varchar();
        }
        else if (field.getType() == int.class) {
            result = DataType.integer();
        }
        else if (field.getType() == Integer.class) {
            result = DataType.integer();
        }
        else if (field.getType() == long.class) {
            result = DataType.longtype();
        }
        else if (field.getType() == Long.class) {
            result = DataType.longtype();
        }
        else if (field.getType() == Timestamp.class) {
            result = DataType.timestamp();
        }
        else if (field.getType() == byte[].class) {
            result = DataType.blob();
        }
        else if (field.getType() == Double.class) {
            result = DataType.doubleType();
        }
        else if (field.getType() == Boolean.class) {
            result = DataType.bool();
        }
        else {
            throw new NotImplementedException();
        }
        return result;
    }

    public static Record toRecord(Object obj) throws IllegalArgumentException, IllegalAccessException {
        HashMapRecord rec = new HashMapRecord();
        Class<?> klass = obj.getClass();
        for (Field i : klass.getFields()) {
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

    public static Cursor toCursor(CursorMeta meta, Map<String, Object> props) {
        props = new TreeMap<>(props);
        Cursor c = new PropertiesCursor(meta, props);
        return c;
    }
}
