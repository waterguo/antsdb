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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.VaporizingRow;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.IndexMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;

/**
 * 
 * @author *-xguo0<@
 */
public class FullTextIndexEntryHandler extends IndexEntryHandler {
    List<ColumnMeta> columns;
    KeyMaker keyMaker = KeyMaker.getFullTextIndexKeyMaker();
    
    public FullTextIndexEntryHandler(GTable gindex, TableMeta table, IndexMeta index) {
        super(gindex, table, index);
        this.columns = index.getColumns(table);
    }

    @Override
    void insert(Heap heap, Transaction trx, VaporizingRow row, int timeout, boolean isReplace) {
        HashMap<String, AtomicInteger> terms = new HashMap<>();
        for (ColumnMeta i:this.columns) {
            String text = (String)row.get(i.getColumnId());
            if (text == null) {
                return;
            }
            LuceneUtil.tokenize(text, (type, term) -> {
                term = term.toLowerCase();
                AtomicInteger count = terms.get(term);
                if (count == null) {
                    count = new AtomicInteger(0);
                    terms.put(term, count);
                }
                count.incrementAndGet();
            });
        }
        VaporizingRow frow = new VaporizingRow(heap, 1);
        frow.setFieldAddress(0, row.getFieldAddress(0));
        for (Map.Entry<String, AtomicInteger> term:terms.entrySet()) {
            frow.set(1, term.getKey());
            long pIndexKey = this.keyMaker.make(heap, frow);
            int count = term.getValue().get();
            byte misc = (byte)((count > 0xff) ? 0xff : count);
            insert(heap, trx, pIndexKey, row.getKeyAddress(), misc, timeout, isReplace);
        }
    }

    @Override
    void delete(Heap heap, Transaction trx, Row row, int timeout) {
        HashSet<String> terms = new HashSet<>();
        for (ColumnMeta i:this.columns) {
            String text = (String)row.get(i.getColumnId());
            if (text == null) {
                return;
            }
            LuceneUtil.tokenize(text, (type, term) -> {
                terms.add(term.toLowerCase());
            });
        }
        VaporizingRow frow = new VaporizingRow(heap, 1);
        frow.setFieldAddress(0, row.getFieldAddress(0));
        for (String term:terms) {
            frow.set(1, term);
            long pIndexKey = this.keyMaker.make(heap, frow);
            delete(heap, trx, pIndexKey, timeout);
        }
    }

    @Override
    void update(Heap heap, Transaction trx, Row oldRow, VaporizingRow newRow, boolean force, int timeout) {
        delete(heap, trx, oldRow, timeout);
        insert(heap, trx, newRow, timeout, true);
    }

}
