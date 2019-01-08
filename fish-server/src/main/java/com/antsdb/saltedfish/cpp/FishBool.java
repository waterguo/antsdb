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
package com.antsdb.saltedfish.cpp;

public class FishBool {
    public static final boolean get(Heap heap, long address) {
        if (address == 0) {
            throw new IllegalArgumentException();
        }
        byte value = Unsafe.getByte(address + 1);
        return value != 0;
    }
    
    public static final long allocSet(Heap heap, boolean value) {
        long address = heap.alloc(2);
        set(address, value);
        return address;
    }

    public static final void set(long address, boolean value) {
        Unsafe.putByte(address, Value.FORMAT_BOOL);
        Unsafe.putByte(address + 1, value ? (byte)1 : (byte)0);
    }

    public final static int getSize(long pValue) {
        return 2;
    }

}
