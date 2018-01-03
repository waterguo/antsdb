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

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.ScanOptions;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.Session;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.MetadataService;
import com.antsdb.saltedfish.sql.meta.PrimaryKeyMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.sql.planner.SortKey;

public class HumpbackTableScan extends CursorMaker {
	TableMeta table;
    CursorMeta cursorMeta;
    int[] mapping;
    boolean isAsc = true;
    boolean noCache = false;
    
    public HumpbackTableScan(TableMeta table, int makerId) {
    	    this.table = table;
        this.cursorMeta = CursorMeta.from(table);
        this.mapping = new int[this.cursorMeta.fields.size()];
        this.mapping = this.cursorMeta.getHumpbackMapping();
        setMakerId(makerId);
    }
    
    @Override
    public CursorMeta getCursorMeta() {
        return this.cursorMeta;
    }
    
    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        GTable table = ctx.getHumpback().getTable(this.table.getHtableId());
        long options = 0;
        options = this.isAsc ? options : ScanOptions.descending(options);
        options = this.noCache ? ScanOptions.noCache(options) : options;
        Cursor cursor = new DumbCursor(
        		ctx.getSpaceManager(),
                this.cursorMeta, 
                table.scan(ctx.getTransaction().getTrxId(), ctx.getTransaction().getTrxTs(), 0, 0, options), 
                mapping,
                ctx.getCursorStats(makerId));
        return cursor;
    }

    public static Cursor create(Session session, ObjectName tableName, List<String> columns) {
        Transaction trx = session.getTransaction();
        Orca orca = session.getOrca();
        TableMeta tableMeta = orca.getMetaService().getTable(trx, tableName);
        GTable table = orca.getHumpback().getTable(tableName.getNamespace(), tableMeta.getHtableId());
        CursorMeta meta = createCursorMeta(session, tableName, columns);
        int[] mapping = new int[columns.size()];
        for (int i=0; i<mapping.length; i++) {
            ColumnMeta column = tableMeta.getColumn(columns.get(i));
            if (column == null) {
                throw new OrcaException();
            }
            mapping[i] = column.getColumnId();
        }
        Cursor cursor = new DumbCursor(
        		session.getOrca().getSpaceManager(), 
        		meta, 
        		table.scan(trx.getTrxId(), trx.getTrxTs(), true), 
        		mapping,
        		new AtomicLong());
        return cursor;
    }

    public static CursorMeta createCursorMeta(Session session, ObjectName tableName, List<String> columns) {
        Orca orca = session.getOrca();
        MetadataService meta = orca.getMetaService();
        TableMeta table = meta.getTable(session.getTransaction(), tableName);
        if (table == null) {
            throw new OrcaException("table not found: " + tableName);
        }
        CursorMeta cursorMeta = new CursorMeta();
        cursorMeta.source = tableName;
        if (columns != null) {
            for (String i:columns) {
                ColumnMeta column = table.getColumn(i);
                cursorMeta.fields.add(FieldMeta.valueOf(column));
            }
        }
        else {
            for (ColumnMeta i:table.getColumns()) {
                cursorMeta.fields.add(FieldMeta.valueOf(i));
            }
        }
        return cursorMeta;
    }

	@Override
	public String toString() {
        return "Table Scan (" + this.table.getObjectName() + ")";
	}

    @Override
    public boolean setSortingOrder(List<SortKey> order) {
        PrimaryKeyMeta pk = this.table.getPrimaryKey();
        if (pk == null) {
            return false;
        }
        int sort = SortKey.follow(SortKey.from(this.table, pk), order);
        switch (sort) {
        case 1:
            this.isAsc = true;
            return true;
        case -1:
            this.isAsc = false;
            return true;
        default:
            return false;
        }
    }
    
    public void setNoCache(boolean value) {
        this.noCache = value;
    }
}
