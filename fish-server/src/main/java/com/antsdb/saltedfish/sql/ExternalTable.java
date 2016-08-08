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
package com.antsdb.saltedfish.sql;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;

import com.antsdb.saltedfish.nosql.SlowRow;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.sql.vdm.HashMapRecord;
import com.antsdb.saltedfish.sql.vdm.Record;

public abstract class ExternalTable {
    public abstract TableMeta getMeta();
    public abstract Iterator<Record> scan();
    
    static class ClassBasedTable<T> extends ExternalTable {
        TableMeta meta = new TableMeta(new SlowRow(0));
        List<Field> fields = new ArrayList<>();
        Iterable<T> source;

        class MyIterator implements Iterator<Record> {
            Iterator<T> upstream;
            
            @Override
            public boolean hasNext() {
                return this.upstream.hasNext();
            }

            @Override
            public Record next() {
                T obj = this.upstream.next();
                if (obj == null) {
                    return null;
                }
                HashMapRecord rec = new HashMapRecord();
                for (int i=0; i<fields.size(); i++) {
                    Field ii = fields.get(i);
                    try {
                        rec.set(i, ii.get(obj));
                    }
                    catch (Exception x) {
                        rec.set(i, x.toString());
                    }
                }
                return rec;
            }
        }
        
        @Override
        public TableMeta getMeta() {
            return this.meta;
        }

        @Override
        public Iterator<Record> scan() {
            MyIterator it = new MyIterator();
            it.upstream = this.source.iterator();
            return it;
        }
        
    }
    
    public static <T> ExternalTable wrap(Orca orca, String namespace, Class<T> klass, Iterable<T> iterable) {
        ClassBasedTable<T> table = new ClassBasedTable<>();
        table.source = iterable;
        table.meta.setNamespace(namespace)
                  .setTableName(iterable.getClass().getSimpleName());
        List<ColumnMeta> columns = new ArrayList<>();
        for (Field i:klass.getFields()) {
            ColumnMeta column = new ColumnMeta(orca.getTypeFactory(), new SlowRow(0));
            column.setColumnName(i.getName())
                  .setId(table.meta.getColumns().size());
            if (i.getType() == String.class) {
                column.setType(DataType.varchar());
            }
            else if (i.getType() == int.class) {
                column.setType(DataType.integer());
            }
            else if (i.getType() == long.class) {
                column.setType(DataType.longtype());
            }
            else {
                throw new NotImplementedException();
            }
            columns.add(column);
            table.fields.add(i);
        }
        table.meta.setColumns(columns);
        return table;
    }
}
