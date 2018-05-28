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
package com.antsdb.saltedfish.charset;

import java.util.HashMap;
import java.util.Map;

/**
 * 
 * @author wgu0
 */
public class Codecs {
	public final static Decoder UTF8 = new Utf8();
	public final static Decoder ISO8859 = new Iso8859();
	public final static Map<String, Decoder> DECODER_BY_NAME = new HashMap<>();
	
	static {
	    DECODER_BY_NAME.put("utf8", UTF8);
	    DECODER_BY_NAME.put("iso8859", ISO8859);
        DECODER_BY_NAME.put("latin1", ISO8859);
        DECODER_BY_NAME.put("cp1250", ISO8859);
        DECODER_BY_NAME.put("utf8mb4", UTF8);
	}
	
	/**
	 * @param name in upper case
	 * @return
	 */
	public static Decoder get(String name) {
	    return DECODER_BY_NAME.get(name.toLowerCase());
	}
}
