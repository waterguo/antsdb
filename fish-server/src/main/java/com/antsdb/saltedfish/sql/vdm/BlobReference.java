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
package com.antsdb.saltedfish.sql.vdm;

import com.antsdb.saltedfish.cpp.BrutalMemoryObject;
import com.antsdb.saltedfish.cpp.Bytes;
import com.antsdb.saltedfish.cpp.FishUtf8;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.cpp.Unicode16;
import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.cpp.Value;

/**
 * 
 * @author *-xguo0<@
 */
public final class BlobReference extends BrutalMemoryObject {
    private static final int OFFSET_SIZE = 1;
    private static final int OFFSET_ROWKEY = 5;
    
    public BlobReference(long addr) {
        super(addr);
        if (Unsafe.getByteVolatile(addr) != Value.FORMAT_BLOB_REF) {
            throw new IllegalArgumentException();
        }
    }
    
    public static BlobReference alloc(Heap heap, long pKey, int dataSize) {
        int keySize = KeyBytes.getRawSize(pKey);
        long pResult = heap.alloc(5 + keySize);
        Unsafe.putByte(pResult, Value.FORMAT_BLOB_REF);
        Unsafe.copyMemory(pKey, pResult + OFFSET_ROWKEY, keySize);
        BlobReference result = new BlobReference(pResult);
        result.setDataSize(dataSize);
        return result;
    }
    
    public static BlobReference alloc(Heap heap, long pKey, long pValue) {
        if (pValue == 0) {
            throw new IllegalArgumentException();
        }
        byte format = Value.getFormat(null, pValue);
        int keySize = KeyBytes.getRawSize(pKey);
        int dataSize;
        if (format == Value.FORMAT_UTF8) {
            dataSize = FishUtf8.getSize(Value.FORMAT_UTF8, pValue);
        }
        else if (format == Value.FORMAT_BYTES) {
            dataSize = Bytes.getRawSize(pValue);
        }
        else if (format == Value.FORMAT_UNICODE16) {
            dataSize = Unicode16.getSize(Value.FORMAT_UNICODE16, pValue);
        }
        else {
            throw new IllegalArgumentException();
        }
        long pResult = heap.alloc(5 + keySize);
        Unsafe.putByte(pResult, Value.FORMAT_BLOB_REF);
        Unsafe.copyMemory(pKey, pResult + OFFSET_ROWKEY, keySize);
        BlobReference result = new BlobReference(pResult);
        result.setDataSize(dataSize);
        return result;
    }
    
    public int getSize() {
        int keySize = KeyBytes.getRawSize(getRowKeyAddress());
        int result = keySize + 5;
        return result;
    }
    
    public int getDataSize() {
        return Unsafe.getInt(this.addr + OFFSET_SIZE);
    }
    
    public void setDataSize(int size) {
        Unsafe.putInt(this.addr + OFFSET_SIZE, size);
    }
    
    public long getRowKeyAddress() {
        return this.addr + OFFSET_ROWKEY;
    }

    @Override
    public String toString() {
        return "BLOB|" + getDataSize();
    }

    @Override
    public int getByteSize() {
        return getSize();
    }

    @Override
    public int getFormat() {
        return Value.FORMAT_BLOB_REF;
    }
}
