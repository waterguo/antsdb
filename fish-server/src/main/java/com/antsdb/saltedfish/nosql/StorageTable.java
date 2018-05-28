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
package com.antsdb.saltedfish.nosql;

import java.util.List;

import com.antsdb.saltedfish.cpp.FileOffset;

/**
 * 
 * @author *-xguo0<@
 */
public interface StorageTable {
    public long get(long pKey);
    public boolean exist(long pKey);
    public long getIndex(long pKey);
    public ScanResult scan(long pKeyStart, long pKeyEnd, long options);
    public void delete(long pKey);
    public void putIndex(long pIndexKey, long pRowKey, byte misc);
    public void put(Row row);
    public String getLocation(long pKey);
    public boolean traceIo(long pKey, List<FileOffset> lines);
}
