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

import java.nio.ByteBuffer;
import java.util.function.IntSupplier;

/**
 * 
 * @author wgu0
 */
public interface Decoder {
    public final static Decoder UTF8 = new Utf8();
    
    public int get(ByteBuffer buf);
    public IntSupplier mapDecode(IntSupplier supplier);
    public int get(IntSupplier supplier);
    /** high 16 bits is the length, low 16 bits is the char */
    public int getChar(long addr);
    
    public default String toString(IntSupplier input) {
        StringBuilder buf = new StringBuilder();
        IntSupplier output = mapDecode(input);
        for (int ch = output.getAsInt(); ch != -1; ch=output.getAsInt()) {
               buf.append((char)ch); 
        }
        return buf.toString();
    }
}
