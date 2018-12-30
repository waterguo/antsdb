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
package com.antsdb.saltedfish.backup;

import java.util.ArrayList;
import java.util.List;

/**
 * 
 * @author *-xguo0<@
 */
public class InsertBuilder {
    public String catalog;
    public String schema;
    public String table;
    public List<String> columns = new ArrayList<>();
    
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("INSERT INTO ");
        append(buf, catalog);
        append(buf, schema);
        append(buf, table);
        buf.append(" VALUES (");
        for (int i=0; i<this.columns.size(); i++) {
            if (i > 0) {
                buf.append(',');
            }
            buf.append('?');
        }
        buf.append(")");
        return buf.toString();
    }

    private void append(StringBuilder buf, String value) {
        if (value != null) {
            if (buf.charAt(buf.length()-1) != ' ') {
                buf.append('.');
            }
            buf.append("`");
            buf.append(value);
            buf.append("`");
        }
    }
}
