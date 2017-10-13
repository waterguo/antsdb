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
package com.antsdb.saltedfish.nosql;

import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.util.BytesUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class ScanResultComparator {
    public static boolean compare(ScanResult x, ScanResult y, TableType type) {
        try {
            for (;;) {
                boolean xeof = x.next();
                boolean yeof = y.next();
                if (!xeof || !yeof) {
                    break;
                }
                boolean result = compare_(x, y, type);
                if (!result) {
                    String xx = rowdump(x.getRowPointer());
                    System.out.println(xx);
                    String yy = rowdump(y.getRowPointer());
                    System.out.println(yy);
                }
                return result;
            }
            return x.eof() && y.eof();
        }
        finally {
            x.close();
            y.close();
        }
    }

    private static String rowdump(long pRow) {
        if (pRow == 0) {
            return ("null");
        }
        int size = Row.getLength(pRow);
        String dump = BytesUtil.toHex(pRow, size);
        return dump;
    }
    
    private static boolean compare_(ScanResult x, ScanResult y, TableType type) {
        while (x.next()) {
            if (!y.next()) {
                return false;
            }
            long pKeyX = x.getKeyPointer();
            long pKeyY = y.getKeyPointer();
            if (KeyBytes.compare(pKeyX, pKeyY) != 0) {
                return false;
            }
            if (type == TableType.DATA) {
                long pRowX = x.getRowPointer();
                long pRowY = y.getRowPointer();
                if (!Row.compareByBytes(pRowX, pRowY)) {
                    return false;
                }
            }
            else {
                long pRowKeyX = x.getIndexRowKeyPointer();
                long pRowKeyY = y.getIndexRowKeyPointer();
                if (KeyBytes.compare(pRowKeyX, pRowKeyY) != 0) {
                    return false;
                }
                if (x.getMisc() != y.getMisc()) {
                    return false;
                }
            }
        }
        if (y.next()) {
            return false;
        }
        return true;
    }
}
