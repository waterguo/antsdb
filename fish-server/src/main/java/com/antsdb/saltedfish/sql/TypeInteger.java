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

/**
 * all integer like types
 *  
 * @author wgu0
 */
public class TypeInteger extends DataType {
    long max;
    long min;
    
    public TypeInteger(String name, int sqlType, Class<?> klass, byte fishType, int length, long max, long min) {
        super(name, length, 0, sqlType, klass, fishType, Weight.INT8);
        this.max = max;
        this.min = min;
    }
}