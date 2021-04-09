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
package com.antsdb.saltedfish.sql.mysql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.RowIterator;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.meta.MetadataService;
import com.antsdb.saltedfish.sql.vdm.Cursor;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.SysColumnRow;
import com.antsdb.saltedfish.sql.vdm.Transaction;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.sql.vdm.View;
import com.antsdb.saltedfish.util.CursorUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class COLUMNS extends View {
    Orca orca;

    public static class Item {
        public String TABLE_CATALOG = "def";
        public String TABLE_SCHEMA;
        public String TABLE_NAME;
        public String COLUMN_NAME;
        public Integer ORDINAL_POSITION;
        public String COLUMN_DEFAULT;
        public String IS_NULLABLE;
        public String DATA_TYPE;
        public Long CHARACTER_MAXIMUM_LENGTH;
        public Long CHARACTER_OCTET_LENGTH;
        public Integer NUMERIC_PRECISION;
        public Integer NUMERIC_SCALE;
        public Long DATETIME_PRECISION;
        public String CHARACTER_SET_NAME;
        public String COLLATION_NAME;
        public String COLUMN_TYPE;
        public String COLUMN_KEY;
        public String EXTRA;
        public String PRIVILEGES;
        public String COLUMN_COMMENT;
    }

    public COLUMNS(Orca orca) {
        super(CursorUtil.toMeta(Item.class));
        this.orca = orca;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        ArrayList<Item> list = new ArrayList<>();
        Transaction trx = Transaction.getSeeEverythingTrx();
        MetadataService metaService = this.orca.getMetaService();
        RowIterator iter;
        iter = metaService.getSysColumn().scan(trx.getTrxId(), trx.getTrxTs(), true);
        Map<Integer, AtomicInteger> counters = new HashMap<>(); 
        for (; iter.next();) {
            Row row = iter.getRow();
            Item item = toItem(new SysColumnRow(row), counters);
            list.add(item);
        }
        Cursor c = CursorUtil.toCursor(meta, list);
        return c;
    }

    private Item toItem(SysColumnRow row, Map<Integer, AtomicInteger> counters) {
        Item item = new Item();
        item.TABLE_SCHEMA = row.getNamespace();
        item.TABLE_NAME = row.getTableName();
        item.COLUMN_NAME = row.getColumnName();
        item.DATA_TYPE = row.getTypeName();
        item.COLUMN_TYPE = item.DATA_TYPE;
        item.NUMERIC_PRECISION = (int)row.getLength();
        item.NUMERIC_SCALE = row.getScale();
        item.IS_NULLABLE = row.isNullable() ? "YES" : "NO";
        AtomicInteger counter = counters.get(row.getTableId());
        if (counter == null) {
            counter = new AtomicInteger();
            counters.put(row.getTableId(), counter);
        }
        item.ORDINAL_POSITION = counter.incrementAndGet();
        return item;
    }
}
