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

import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.FishUtf8;
import com.antsdb.saltedfish.cpp.Value;
import com.antsdb.saltedfish.sql.vdm.Record;

/**
 * 
 * @author wgu0
 */
public class debug {
	public static String dump(long p) {
		byte type = Value.getFormat(null, p);
		if (type == Value.FORMAT_RECORD) {
			return Record.dump(p);
		}
		else if (type == Value.FORMAT_UTF8) {
			String s = FishUtf8.get(p);
			return s;
		}
		return FishObject.debug(p);
	}
}
