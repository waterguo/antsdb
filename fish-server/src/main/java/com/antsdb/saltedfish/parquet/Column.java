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
package com.antsdb.saltedfish.parquet;

/**
 * 
 * @author Frank Li<lizc@tg-hd.com>
 */
public class Column {

    private String name;
    private int type;

    private Column(String name, int type) {
        this.name = name;
        this.type = type;
    }

    public static Column valueOf(String name, int type) {
        return new Column(name, type);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other instanceof Column) {
            Column anotherObj = (Column) other;
            if (this.name.equals(anotherObj.name)) {
                return true;
            }
        }
        return false;
    }

    public String toString() {
        return "Name :" + this.name + "\tThype:" + type;
    }

}
