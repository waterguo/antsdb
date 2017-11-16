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
package com.antsdb.saltedfish.sql.mysql;

import java.util.ArrayList;

import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.RowIterator;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.meta.ColumnId;
import com.antsdb.saltedfish.sql.meta.MetadataService;
import com.antsdb.saltedfish.sql.vdm.Cursor;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.Transaction;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.sql.vdm.ViewMaker;
import com.antsdb.saltedfish.util.CursorUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class COLUMNS extends ViewMaker {
    Orca orca;

    public static class Item {
        public String TABLE_CATALOG = "def";
        public String TABLE_SCHEMA;
        public String TABLE_NAME;
        public String COLUMN_NAME;
        public Long ORDINAL_POSITION;
        public String COLUMN_DEFAULT;
        public String IS_NULLABLE;
        public String DATA_TYPE;
        public Long CHARACTER_MAXIMUM_LENGTH;
        public Long CHARACTER_OCTET_LENGTH;
        public Long NUMERIC_PRECISION;
        public Long NUMERIC_SCALE;
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
        RowIterator iter = metaService.getSysColumn().scan(trx.getTrxId(), trx.getTrxTs(), true);
        for (; iter.next();) {
            Row row = iter.getRow();
            Item item = toItem(row);
            list.add(item);
        }
        Cursor c = CursorUtil.toCursor(meta, list);
        return c;
    }

    private Item toItem(Row row) {
        Item item = new Item();
        item.TABLE_SCHEMA = (String) row.get(ColumnId.syscolumn_namespace.getId());
        item.TABLE_NAME = (String) row.get(ColumnId.syscolumn_table_name.getId());
        item.COLUMN_NAME = (String) row.get(ColumnId.syscolumn_column_name.getId());
        item.DATA_TYPE = (String) row.get(ColumnId.syscolumn_type_name.getId());
        item.COLUMN_TYPE = item.DATA_TYPE;
        item.NUMERIC_PRECISION = (long) (Integer) row.get(ColumnId.syscolumn_type_length.getId());
        item.NUMERIC_SCALE = (long) (Integer) row.get(ColumnId.syscolumn_type_scale.getId());
        return item;
    }
}
