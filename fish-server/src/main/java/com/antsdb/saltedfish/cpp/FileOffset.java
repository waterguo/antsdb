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
package com.antsdb.saltedfish.cpp;

import java.io.File;

/**
 * 
 * @author *-xguo0<@
 */
public class FileOffset {
    public File file;
    public long offset;
    public String note = "";
    
    public FileOffset(File file, long offset, String note) {
        this.file = file;
        this.offset = offset;
        this.note = note;
    }

    public FileOffset setNote(String value) {
        this.note = value;
        return this;
    }
}
