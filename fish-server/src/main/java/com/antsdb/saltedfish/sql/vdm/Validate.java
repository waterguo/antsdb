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
package com.antsdb.saltedfish.sql.vdm;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.Value;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * validate the content of a table
 * 
 * @author xinyi
 *
 */
public class Validate extends Statement {
    static Logger _log = UberUtil.getThisLogger();

    ObjectName tableName;
    
    
    public Validate(ObjectName table) {
        super();
        this.tableName = table;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params) {
        TableMeta table = Checks.tableExist(ctx.session, this.tableName);
        List<String> columns = new ArrayList<String>();
        table.getColumns().forEach(it -> { columns.add(it.getColumnName());});
        Cursor c = HumpbackTableScan.create(ctx.getSession(), this.tableName, columns);
        boolean isOk = true;
        for (long pRecord = c.next(); pRecord != 0; pRecord=c.next()) {
            for (int i=0; i<table.getColumns().size(); i++) {
                ColumnMeta ii = table.getColumns().get(i);
                try {
                    long pValue = Record.get(pRecord, i);
                    if (ii.getDataType().getFishType() == Value.getFormat(null, pValue)) {
                        continue;
                    }
                }
                catch (Exception x) {
                    _log.warn("error:", x);
                }
                isOk = false;
                _log.warn("invalid value found on table {} column {} key {}", 
                        this.tableName, 
                        ii.getColumnName(),
                        FishObject.get(null, Record.getKey(pRecord)));
            }
        }
        return isOk;
    }
    
}
