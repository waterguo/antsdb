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

public class Aliaser extends CursorMaker {
    String alias;
    CursorMaker upstream;
    CursorMeta meta;
    
    class AliasedCursor extends Cursor {
        Cursor upstream;
        
        public AliasedCursor(Cursor upstream) {
            super(Aliaser.this.meta);
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
    
    public Aliaser(String alias, CursorMaker upstream) {
        super();
        this.alias = alias;
        this.upstream = upstream;
        this.meta = new CursorMeta();
        this.meta.source = upstream.getCursorMeta().source;
        for (FieldMeta i:upstream.getCursorMeta().getColumns()) {
            FieldMeta column = new FieldMeta(i.getName(), i.getType());
            column.setTableAlias(alias);
            this.meta.fields.add(column);
        }
    }

    @Override
    public CursorMeta getCursorMeta() {
        return this.meta;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pRecord) {
        Cursor c = this.upstream.make(ctx, params, pRecord);
        return new AliasedCursor(c);
    }

    @Override
    public void explain(int level, List<ExplainRecord> records) {
        this.upstream.explain(level, records);
    }

}
