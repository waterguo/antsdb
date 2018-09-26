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

import java.io.Closeable;

/**
 * pointer to a record
 * 
 * @author wgu0
 */
public abstract class Cursor implements Closeable {
    CursorMeta meta;
    String name;
    CursorMaker maker;

    public abstract long next();
    public abstract void close();

    public Cursor(CursorMaker maker) {
        this(maker.getCursorMeta());
        this.maker = maker;
    }
    
    public Cursor(CursorMeta meta) {
        this.meta = meta;
    }

    public CursorMeta getMetadata() {
        return meta;
    }
    
    public String getName() {
        if (this.name != null) {
            return this.name;
        }
        if (this.maker != null) {
            return this.maker.toString();
        }
        return "";
    }
    
    public void setName(String name) {
        this.name = name;
    }
    
}
