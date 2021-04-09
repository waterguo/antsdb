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

import java.sql.Timestamp;
import java.util.ArrayList;

import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.RowIterator;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.meta.ColumnId;
import com.antsdb.saltedfish.sql.vdm.Cursor;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.Transaction;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.sql.vdm.View;
import com.antsdb.saltedfish.util.CursorUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class TABLES extends View {
    Orca orca;

    public static class Item {
        public String TABLE_SCHEMA_CATALOG = "def";
        public String TABLE_SCHEMA;
        public String TABLE_NAME;
        public String TABLE_TYPE;
        public String ENGINE;
        public Integer VERSION;
        public String ROW_FORMAT;
        public Long TABLE_ROWS;
        public Integer AVG_ROW_LENGTH;
        public Integer DATA_LENGTH;
        public Integer MAX_DATA_LENGTH;
        public Integer INDEX_LENGTH;
        public Integer DATA_FREE;
        public Integer AUTO_INCREMENT;
        public Timestamp CREATE_TIME;
        public Timestamp UPDATE_TIME;
        public Timestamp CHECK_TIME;
        public String TABLE_COLLATION;
        public Long CHECKSUM;
        public String CREATE_OPTIONS;
        public String TABLE_COMMENT;
    }

    public TABLES(Orca orca) {
        super(CursorUtil.toMeta(Item.class));
        this.orca = orca;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        ArrayList<Item> list = new ArrayList<>();
        Transaction trx = Transaction.getSeeEverythingTrx();
        RowIterator iter = ctx.getMetaService().getSysTable().scan(trx.getTrxId(), trx.getTrxTs(), true);
        for (; iter.next();) {
            Row row = iter.getRow();
            Item item = toItem(row);
            list.add(item);
        }
        addInformationSchema(list);
        Cursor c = CursorUtil.toCursor(meta, list);
        return c;
    }

    private void addInformationSchema(ArrayList<Item> list) {
        list.add(addInformationTable("SCHEMATA"));
        list.add(addInformationTable("TABLES"));
    }

    private Item addInformationTable(String string) {
        Item item = new Item();
        item.TABLE_SCHEMA = "information_schema";
        item.TABLE_NAME = string;
        item.TABLE_TYPE = "SYSTEM VIEW";
        item.ENGINE = "MEMORY";
        item.VERSION = 10;
        item.TABLE_COLLATION = "utf8_general_ci";
        return item;
    }

    private Item toItem(Row row) {
        Item item = new Item();
        item.TABLE_SCHEMA = (String)row.get(ColumnId.systable_namespace.getId());
        item.TABLE_NAME = (String)row.get(ColumnId.systable_table_name.getId());
        item.TABLE_TYPE = "BASE TABLE";
        item.ENGINE = "InnoDB";
        item.VERSION = 10;
        item.TABLE_COLLATION = "utf8_general_ci";
        item.TABLE_COMMENT = (String)row.get(ColumnId.systable_comment.getId());
        return item;
    }
}
