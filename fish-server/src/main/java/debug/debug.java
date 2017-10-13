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
package debug;

import java.sql.Timestamp;

import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.FishTimestamp;
import com.antsdb.saltedfish.cpp.FishUtf8;
import com.antsdb.saltedfish.cpp.Int4Array;
import com.antsdb.saltedfish.cpp.Value;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.sql.vdm.Record;

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
        else if (type == Value.FORMAT_TIMESTAMP) {
            Timestamp val = FishTimestamp.get(null, p);
            return val.toString();
        }
        else if (type == Value.FORMAT_INT4_ARRAY) {
            return "[" + Integer.toHexString(Value.FORMAT_INT4_ARRAY & 0xff) + "]" + new Int4Array(p).toString();
        }
		return FishObject.debug(p);
	}
}
