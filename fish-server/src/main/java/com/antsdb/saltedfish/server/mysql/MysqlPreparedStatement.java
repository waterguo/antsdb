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
package com.antsdb.saltedfish.server.mysql;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import com.antsdb.saltedfish.cpp.Bytes;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.MemoryManager;
import com.antsdb.saltedfish.nosql.VaporizingRow;
import com.antsdb.saltedfish.sql.PreparedStatement;
import com.antsdb.saltedfish.sql.Session;
import com.antsdb.saltedfish.sql.vdm.FishParameters;
import com.antsdb.saltedfish.util.UberUtil;

import io.netty.buffer.ByteBuf;

/**
 * @author xgu0
 */
public final class MysqlPreparedStatement implements Closeable {

    public byte[] types;
    PreparedStatement script;
    ByteBuffer meta;
    int packetSequence;
    Map<Integer, ByteBuffer> longdata;
    
    public MysqlPreparedStatement(PreparedStatement script) {
        super();
        this.script = script;
    }

    public int getId() {
        return script.hashCode();
    }

    public int getParameterCount() {
        return this.script.getParameterCount();
    }

    public Object run(Session session, VaporizingRow row, Consumer<Object> callback) {
        FishParameters params = new FishParameters(row);
        Object result = session.run(this.script, params, callback);
        return result;
    }

    @Override
    public void close() {
        clear();
    }

    public void clear() {
        if (this.longdata != null) {
            for (ByteBuffer i:this.longdata.values()) {
                MemoryManager.free(i);
            }
        }
        this.longdata = null;
    }

    public ByteBuf getLongData(int i) {
        return null;
    }
    
    public CharBuffer getSql() {
        return this.script.getSql();
    }
    
    public void setLongData(int paramId, long pSourceData, int dataLen) {
        if (this.longdata == null) {
            this.longdata = new HashMap<>();
        }
        ByteBuffer buf = MemoryManager.alloc(dataLen + 4);
        Bytes.set(buf, pSourceData, dataLen);
        this.longdata.put(paramId, buf);
    }
    
    public void preExecute(VaporizingRow row) {
        if (this.longdata == null) {
            return;
        }
        for (Map.Entry<Integer, ByteBuffer> i:this.longdata.entrySet()) {
            long pValue = UberUtil.getAddress(i.getValue());
            row.setFieldAddress(i.getKey(), pValue);
        }
    }

    public VaporizingRow createRow(Heap heap2) {
        return new VaporizingRow(heap2, script.getParameterCount()-1);
    }
}