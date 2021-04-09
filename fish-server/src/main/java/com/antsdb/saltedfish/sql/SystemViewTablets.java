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
package com.antsdb.saltedfish.sql;

import java.util.ArrayList;

import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.MemTablet;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.sql.vdm.Cursor;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.Transaction;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.sql.vdm.View;
import com.antsdb.saltedfish.util.CursorUtil;
import com.antsdb.saltedfish.util.LongLong;
import static com.antsdb.saltedfish.util.UberFormatter.*;

/**
 * 
 * @author wgu0
 */
public class SystemViewTablets extends View {
    Orca orca;
    
    public static class Item {
        public String NAMESPACE;
        public String TABLE_NAME;
        public int TABLE_ID;
        public int TABLET_ID;
        public String FILE;
        public String START_SP;
        public String END_SP;
        public long START_TRXID;
        public long END_TRXID;
        public int CARBONFROZEN;
    }

    public SystemViewTablets(Orca orca) {
        super(CursorUtil.toMeta(Item.class));
        this.orca = orca;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        ArrayList<Item> list = new ArrayList<>();
        for (GTable table:this.orca.getHumpback().getTables()) {
            for (MemTablet tablet:table.getTablets()) {
                addItem(list, table, tablet);
            }
        }
        
        // done
        Cursor c = CursorUtil.toCursor(meta, list);
        return c;
    }

    private void addItem(ArrayList<Item> list, GTable gtable, MemTablet tablet) {
        TableMeta table = this.orca.getMetaService().getTable(Transaction.getSeeEverythingTrx(), gtable.getId());
        Item item = new Item();
        list.add(item);
        item.NAMESPACE = gtable.getNamespace();
        item.TABLE_NAME = (table != null) ? table.getTableName() : null;
        item.TABLE_ID = gtable.getId();
        item.TABLET_ID = tablet.getTabletId();
        item.FILE = tablet.getFile().getAbsolutePath();
        LongLong span = tablet.getLogSpan();
        item.START_SP = (span != null) ? hex(span.x) : "";
        item.END_SP = (span != null) ? hex(span.y) : "";
        item.START_TRXID = tablet.getStartTrxId();
        item.END_TRXID = tablet.getEndTrxId();
        item.CARBONFROZEN = tablet.isCarbonfrozen() ? 1 : 0;
    }
}
