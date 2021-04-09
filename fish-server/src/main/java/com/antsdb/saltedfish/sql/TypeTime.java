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

import java.sql.Time;
import java.sql.Types;

import com.antsdb.saltedfish.cpp.Value;

/**
 * 
 * @author wgu0
 */
public class TypeTime extends DataType {
    public TypeTime(String name) {
        super(name, 0, 0, Types.TIME, Time.class, Value.FORMAT_TIME, Weight.TIME);
    }
}
