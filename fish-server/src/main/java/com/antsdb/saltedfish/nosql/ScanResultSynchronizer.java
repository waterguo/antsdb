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

import java.util.function.Supplier;

/**
 * 
 * @author *-xguo0<@
 */
public class ScanResultSynchronizer {
    public static int synchronize(RowIterator source, StorageTable target, TableType type, Supplier<Long> callback) {
        int count = 0;
        while (source.next()) {
            synchronizeSingleEntry(source, target, type);
            if (callback != null) {
                callback.get();
            }
            count++;
        }
        return count;
    }

    public static void synchronizeSingleEntry(RowIterator source, StorageTable target, TableType type) {
        long pRow = source.getRowPointer();
        if (Row.isTombStone(pRow)) {
            target.delete(source.getKeyPointer());
            return;
        }
        if (type == TableType.DATA) {
            Row row = Row.fromMemoryPointer(pRow, source.getVersion());
            target.put(row);
        }
        else {
            long pIndexKey = source.getKeyPointer();
            long pRowKey = source.getRowKeyPointer();
            byte misc = source.getMisc();
            target.putIndex(pIndexKey, pRowKey, misc);
        }
    }
    
    public static int synchronize(ScanResult source, StorageTable target, TableType type) {
        int count = 0;
        while (source.next()) {
            synchronizeSingleEntry(source, target, type);
            count++;
        }
        return count;
    }

    public static void synchronizeSingleEntry(ScanResult source, StorageTable target, TableType type) {
        long pRow = source.getRowPointer();
        if (Row.isTombStone(pRow)) {
            target.delete(source.getKeyPointer());
        }
        else {
            if (type == TableType.DATA) {
                Row row = Row.fromMemoryPointer(pRow, source.getVersion());
                target.put(row);
            }
            else {
                long pIndexKey = source.getKeyPointer();
                long pRowKey = source.getIndexRowKeyPointer();
                byte misc = source.getMisc();
                target.putIndex(pIndexKey, pRowKey, misc);
            }
        }
    }
    
    static boolean verify(long pRow, long pKey, StorageTable target, TableType type) {
        long pResult = target.get(pKey);
        if (Row.isTombStone(pRow)) {
            if (pResult == 0) {
                return true;
            }
        }
        else if (pResult != 0) {
            return true;
        }
        return false;
    }
}
