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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.function.IntSupplier;

import com.antsdb.saltedfish.charset.Decoder;
import com.antsdb.saltedfish.charset.Utf8;
import com.antsdb.saltedfish.cpp.Bytes;
import com.antsdb.saltedfish.cpp.FishUtf8;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.sql.DataType;

/**
 * mysql specific
 *   
 * @author *-xguo0<@
 */
public class BinaryString extends Operator {
    
    private byte[] bytes;
    private boolean isBinary;

    public BinaryString(byte[] bytes, boolean isBinary) {
        this.bytes = bytes;
        this.isBinary = isBinary;
    }

    public boolean isBinary() {
        return isBinary;
    }

    public void setBinary(boolean isBinary) {
        this.isBinary = isBinary;
    }

    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        if (this.bytes == null) {
            return 0;
        }
        if (this.isBinary) {
            return Bytes.allocSet(heap, this.bytes);
        }
        else {
            Decoder decoder = ctx.getSession().getConfig().getRequestDecoder();
            IntSupplier supplier = decoder.mapDecode(new IntSupplier() {
                int idx=0;
                @Override
                public int getAsInt() {
                    if (idx >= bytes.length) {
                        return -1;
                    }
                    return bytes[this.idx++] & 0xff;
                }
            });
            long pBuffer = heap.alloc(this.bytes.length * 4);
            AtomicInteger idx = new AtomicInteger();
            IntConsumer consumer = new IntConsumer() {
                @Override
                public void accept(int value) {
                    Unsafe.putByte(pBuffer + idx.getAndIncrement(), (byte)value);
                }
            };
            Utf8.encode(supplier, consumer);
            if (idx.get() > (this.bytes.length * 4)) {
                throw new IllegalArgumentException();
            }
            long pResult = FishUtf8.allocSet(heap, pBuffer, idx.get());
            return pResult;
        }
    }

    public long getBytes(Heap heap) {
        return Bytes.allocSet(heap, this.bytes);
    }
    
    @Override
    public DataType getReturnType() {
        return this.isBinary ? DataType.blob() : DataType.varchar();
    }

    @Override
    public void visit(Consumer<Operator> visitor) {
        visitor.accept(this);
    }

    @Override
    public String toString() {
        return new String(this.bytes);
    }
}
