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
import java.util.Arrays;
import java.util.List;

public class Parameters {
    public List<Object> values = new ArrayList<Object>();
    
    public Parameters(Object[] params) {
        this.values.addAll(Arrays.asList(params));
    }

    public Parameters() {
    }

    public Object get(int pos) {
        return this.values.get(pos);
    }

    public int size() {
        return this.values.size();
    }
    
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append('[');
        for (Object val:this.values) {
            if (val == null) {
                buf.append("NULL");
            }
            else {
            	String text = val.toString();
            	if (text.length() >= 20) {
            		text = text.subSequence(0, 20) + " ...";
            	}
                buf.append(text);
            }
            buf.append(',');
        }
        buf.deleteCharAt(buf.length()-1);
        buf.append(']');
        return buf.toString();
    }
    
}
