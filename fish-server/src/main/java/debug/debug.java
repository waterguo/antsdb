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
package debug;

import java.nio.CharBuffer;
import java.sql.Timestamp;

import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.FishTimestamp;
import com.antsdb.saltedfish.cpp.FishUtf8;
import com.antsdb.saltedfish.cpp.Int4Array;
import com.antsdb.saltedfish.cpp.Value;
import com.antsdb.saltedfish.nosql.IndexRow;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.sql.vdm.Record;
import com.antsdb.saltedfish.util.BytesUtil;

/**
 * 
 * @author wgu0
 */
public class debug {
    public static String dump(long p) {
        if (p <= 0x10) {
            return "illegal memory address: " + p;
        }
        byte type = Value.getFormat(null, p);
        if (type == Value.FORMAT_RECORD) {
            return Record.dump(p);
        }
        else if (type == Value.FORMAT_UTF8) {
            String s = FishUtf8.get(p);
            return s;
        }
        else if (type == Value.FORMAT_ROW) {
            String s = Row.fromMemoryPointer(p, Row.getVersion(p)).toString();
            return s;
        }
        else if (type == Value.FORMAT_INDEX_ROW) {
            String s = new IndexRow(p).toString();
            return s;
        }
        else if (type == Value.FORMAT_TIMESTAMP) {
            Timestamp val = FishTimestamp.get(null, p);
            return val.toString();
        }
        else if (type == Value.FORMAT_INT4_ARRAY) {
            return "[" + Integer.toHexString(Value.FORMAT_INT4_ARRAY & 0xff) + "]" + new Int4Array(p).toString();
        }
        return FishObject.debug(p);
    }
    
    public static String dumpHex(CharBuffer buf) {
        StringBuilder s = new StringBuilder();
        buf = buf.duplicate();
        buf.position(0);
        for (int i=0; buf.hasRemaining(); i++) {
            char ch = buf.get();
            String hex = BytesUtil.toHex(ch);
            s.append(hex);
            s.append(' ');
            if ((i > 0) && (i % 8 == 0)) {
               buf.append("\n"); 
            }
        }
        return s.toString();
    }
    
    public String dumpHex(String s) {
        return dumpHex(CharBuffer.wrap(s));
    }
}
