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
package com.antsdb.saltedfish.minke;

import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.util.BytesUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class CacheVerificationException extends MinkeException {
    private static final long serialVersionUID = 1L;

    public CacheVerificationException(int tableId, long pKey, long pResult, long pExpected) {
        super(makeMessage(tableId, pKey, pExpected, pResult));
    }

    private static String makeMessage(int tableId, long pKey, long pResult, long pExpected) {
        String result = getRowDump(pResult);
        String expected = getRowDump(pExpected);
        String msg = String.format("%d %s %s %s", tableId, KeyBytes.toString(pKey), result, expected);
        return msg;
    }

    private static String getRowDump(long pRow) {
        if ((pRow == 0) || (pRow == 1)) {
            return String.valueOf(pRow);
        }
        int size = Row.getLength(pRow);
        String result = BytesUtil.toCompactHex(pRow, Math.max(size, 0x20));
        if (size > 0x20) {
            result += "...";
        }
        return result;
    }

}
