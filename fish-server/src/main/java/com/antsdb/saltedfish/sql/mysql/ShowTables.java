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
import java.util.regex.Pattern;

import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.RowIterator;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.Session;
import com.antsdb.saltedfish.sql.vdm.Checks;
import com.antsdb.saltedfish.sql.vdm.Cursor;
import com.antsdb.saltedfish.sql.vdm.CursorMeta;
import com.antsdb.saltedfish.sql.vdm.FieldMeta;
import com.antsdb.saltedfish.sql.vdm.OpLike;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.Script;
import com.antsdb.saltedfish.sql.vdm.SysTableRow;
import com.antsdb.saltedfish.sql.vdm.Transaction;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.sql.vdm.View;
import com.antsdb.saltedfish.util.CursorUtil;

public class ShowTables extends View {
    String namespace;
    boolean isFull;
    String sql;
    Script upstream;
    String like;
    Pattern pattern;

    public static class LineShort {
        public String TABLE_NAME;
    }
    
    public static class LineFull {
        public String TABLE_NAME;
        public String TABLE_TYPE;
    }
    
    static class MetaReplacer extends Cursor {
        private Cursor upstream;

        public MetaReplacer(CursorMeta meta, Cursor upstream) {
            super(meta);
            this.upstream = upstream;
        }

        @Override
        public long next() {
            return this.upstream.next();
        }

        @Override
        public void close() {
            this.upstream.close();
        }
        
    }
    public ShowTables(Session session, String namespace, boolean isFull, String like) {
        super(getMeta(isFull, namespace));
        this.namespace = namespace;
        this.isFull = isFull;
        this.like = like;
        if (like != null) {
            this.pattern = OpLike.compile(like);
        }
    }
    
    public void setLike(String like) {
        this.like = like;
    }

    @Override
    public Object run(VdmContext ctx, Parameters notused, long notused1) {
        List<Object> result = new ArrayList<>();
        GTable systable = ctx.getMetaService().getSysTable();
        Transaction trx = ctx.getTransaction();
        RowIterator iter = systable.scan(trx.getTrxId(), trx.getTrxTs(), true);
        String ns = this.namespace;
        if (ns == null) {
            ns = ctx.getSession().getCurrentNamespace();
            if (ns == null) {
                throw new OrcaException("namespace is not specified");
            }
        }
        else {
            ns = Checks.namespaceExist(ctx.getOrca(), this.namespace);
        }
        while (iter.next()) {
            SysTableRow row = new SysTableRow(iter.getRow());
            if (row.isTemproray()) {
                continue;
            }
            if (!row.getNamespace().equalsIgnoreCase(ns)) {
                continue;
            }
            if (this.pattern != null) {
                if (!pattern.matcher(row.getTableName()).matches()) {
                    continue;
                }
            }
            result.add(isFull ? toFullLine(row) : toShortLine(row));
        }
        CursorMeta originalMeta = this.isFull ? CursorUtil.toMeta(LineFull.class) : CursorUtil.toMeta(LineShort.class);
        Cursor resultCursor = CursorUtil.toCursor(originalMeta, result);
        return new MetaReplacer(this.meta, resultCursor);
    }

    private LineShort toShortLine(SysTableRow row) {
        LineShort result = new LineShort();
        result.TABLE_NAME = row.getTableName();
        return result;
    }

    private LineFull toFullLine(SysTableRow row) {
        LineFull result = new LineFull();
        result.TABLE_NAME = row.getTableName();
        result.TABLE_TYPE = "BASE TABLE";
        return result;
    }
    
    private static CursorMeta getMeta(boolean isFull, String namespace) {
        CursorMeta meta = isFull ? CursorUtil.toMeta(LineFull.class) : CursorUtil.toMeta(LineShort.class);
        FieldMeta field = meta.getColumn(0);
        field.setName("Tables_in_" + namespace);
        return meta;
    }
}
