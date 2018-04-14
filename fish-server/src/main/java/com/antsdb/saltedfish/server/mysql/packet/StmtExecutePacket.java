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
package com.antsdb.saltedfish.server.mysql.packet;

import io.netty.buffer.ByteBuf;

import java.util.BitSet;

import com.antsdb.saltedfish.server.mysql.MysqlServerHandler;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.server.mysql.MysqlPreparedStatement;
import com.antsdb.saltedfish.server.mysql.util.BindValueUtil;
import com.antsdb.saltedfish.server.mysql.util.BufferUtils;
import com.antsdb.saltedfish.server.mysql.util.Fields;
import com.antsdb.saltedfish.sql.OrcaException;

public class StmtExecutePacket extends RecievePacket {
    private ByteBuf content;
    public int statementId;
    private MysqlPreparedStatement pstmt;
    
    public StmtExecutePacket(int command) {
        super(command);
    }

    @Override
    public void read(MysqlServerHandler handler, ByteBuf in) {
        	this.content = in.readSlice(this.packetLength);
        	content.retain();
    }
    
    public void read_(MysqlServerHandler handler) {
        try {
            read__(handler);
        }
        finally {
            this.content.release();
        }
    }
    
    @SuppressWarnings("unused")
    public void read__(MysqlServerHandler handler) {
        ByteBuf in = this.content;
        statementId = (int) BufferUtils.readUB4(in);
        byte flags = BufferUtils.readByte(in);
        long iterationCount = BufferUtils.readUB4(in);
        this.pstmt = handler.getPrepared().get(statementId);
        if (pstmt == null) {
            throw new OrcaException("prepared statement {} is not found", statementId);
        }

        // read null bitmap

        int nullCount = (pstmt.getParameterCount() + 7) / 8;
        byte[] bytes = new byte[nullCount];
        in.readBytes(bytes);
        BitSet nullBits = BitSet.valueOf(bytes);

        // type information

        int sentTypes = in.readByte();
        if (sentTypes != 0) {
            int[] types = new int[pstmt.getParameterCount()];
            for (int i = 0; i < pstmt.getParameterCount(); i++) {
                types[i] = BufferUtils.readInt(in);
            }
            pstmt.types = types;
        }

        // bind values

        if (pstmt.types == null) {
            // type information is supposed to be sent in the first execution.
        }
        Heap heap = pstmt.getHeap();
        for (int i = 0; i < pstmt.getParameterCount(); i++) {
            if (nullBits.get(i)) {
                continue;
            }
            if (pstmt.types[i] == Fields.FIELD_TYPE_BLOB) {
                // BLOB values should be set with long data packet
                // and already in pstmt. use isSet to indicate using
                // long data packet in PreparedStmtHandler in execution
                continue;
            }
            long pValue = BindValueUtil.read(heap, in, pstmt.types[i]);
            pstmt.setParam(i, pValue);
        }
    }

    @Override
    public String toString() {
        String s = super.toString();
        if (this.pstmt != null) {
            s += this.pstmt.getSql();
        }
        return s;
    }

}
