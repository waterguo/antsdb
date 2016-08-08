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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.util.CursorUtil;

public class ShowVariables extends CursorMaker {
    CursorMeta meta = new CursorMeta();

    public ShowVariables() {
    	FieldMeta field = new FieldMeta("Variable_name", DataType.varchar(192));
    	field.setTableAlias("VARIABLES");
    	field.setSourceColumnName("VARIABLE_NAME");
    	field.setSourceTable(new ObjectName("information_schema", "VARIABLES"));
    	meta.addColumn(field);
    	field = new FieldMeta("Value", DataType.varchar(3072));
    	field.setTableAlias("VARIABLES");
    	field.setSourceTable(new ObjectName("information_schema", "VARIABLES"));
    	field.setSourceColumnName("VARIABLE_VALUE");
        meta.addColumn(field);
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        // Map<String, Object> variables = session.getVariables();
        Map<String, Object> variables = new HashMap<String, Object>();
        variables.put("character_set_result", "utf8");
        variables.put("max_allowed_packet", "4194304");
        variables.put("tx_isolation", "READ-COMMITTED");
        variables.put("sql_mode", "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION");
        List<Record> list = new ArrayList<>();
        variables.entrySet().forEach((it) -> {
            Record rec = new HashMapRecord();
            rec.set(0, it.getKey())
               .set(1, it.getValue().toString());
            list.add(rec);
        });
        Cursor c = CursorUtil.toCursor(meta, list);
        return c;
    }

    @Override
    public CursorMeta getCursorMeta() {
        return this.meta;
    }

}
