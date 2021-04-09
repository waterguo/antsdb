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
import java.util.List;

import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.RowIterator;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.meta.OrcaTableType;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.SysTableRow;
import com.antsdb.saltedfish.sql.vdm.Transaction;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.sql.vdm.View;
import com.antsdb.saltedfish.util.CursorUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class VIEWS extends View {

    Orca orca;
    
    public static class Item {
        public String TABLE_CATALOG;
        public String TABLE_SCHEMA;
        public String TABLE_NAME;
        public String VIEW_DEFINITION;
        public String CHECK_OPTION;
        public String IS_UPDATABLE;
        public String DEFINER;
        public String SECURITY_TYPE;
        public String CHARACTER_SET_CLIENT;
        public String COLLATION_CONNECTION;    
    }
    
    public VIEWS(Orca orca) {
        super(CursorUtil.toMeta(Item.class));
        this.orca = orca;
    }
    
    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        GTable systable = ctx.getMetaService().getSysTable();
        Transaction trx = ctx.getTransaction();
        RowIterator iter = systable.scan(trx.getTrxId(), trx.getTrxTs(), true);
        List<Item> result = new ArrayList<>();
        while (iter.next()) {
            SysTableRow row = new SysTableRow(iter.getRow());
            if (row.getType() == OrcaTableType.VIEW) {
                result.add(toLine(ctx, row));
            }
        }
        return CursorUtil.toCursor(getCursorMeta(), result);
    }

    private Item toLine(VdmContext ctx, SysTableRow row) {
        Item result = new Item();
        result.TABLE_CATALOG = "def";
        result.TABLE_SCHEMA = row.getNamespace();
        result.TABLE_NAME = row.getTableName();
        result.VIEW_DEFINITION = "";
        result.CHECK_OPTION = "NONE";
        result.IS_UPDATABLE = "NO";
        result.DEFINER = "@";
        result.SECURITY_TYPE = "DEFINER";
        result.CHARACTER_SET_CLIENT = "utf8";
        result.COLLATION_CONNECTION = "utf8_general_ci";
        return result;
    }
}
