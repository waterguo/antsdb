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

import com.antsdb.saltedfish.cpp.Unsafe;

/**
 * ISO8859 decoder
 *  
 * @author wgu0
 */
public class Iso8859 implements Decoder {

    @Override
    public int get(ByteBuffer buf) {
        int ch = buf.get() & 0xff;
        return ch;
    }

    @Override
    public IntSupplier mapDecode(IntSupplier input) {
        IntSupplier result = new IntSupplier() {
            @Override
            public int getAsInt() {
                int ch = get(input);
                return ch;
            }
        };
        return result;
    }

    @Override
    public int get(IntSupplier input) {
        int result = input.getAsInt();
        if (result == -1) {
            return -1;
        }
        return result & 0xffff;
    }

    @Override
    public int getChar(long addr) {
        int result = Unsafe.getByte(addr) & 0xff;
        result = result | 0x10000;
        return result;
    }

}
