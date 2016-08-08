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

import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.Bytes;
import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.server.mysql.MysqlPreparedStatement;
import com.antsdb.saltedfish.server.mysql.MysqlServerHandler;
import com.antsdb.saltedfish.server.mysql.util.BufferUtils;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.util.UberUtil;

import io.netty.buffer.ByteBuf;

public class LongDataPacket extends RecievePacket {
    static Logger _log = UberUtil.getThisLogger();
    
	private ByteBuf content;
    
    public LongDataPacket(int command) {
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

    public void read__(MysqlServerHandler handler) {
        int stmtId = BufferUtils.readLong(this.content);
		MysqlPreparedStatement pstmt = handler.getPrepared().get(stmtId);
		if (pstmt == null) {
			throw new OrcaException("prepared statement %d is not found", stmtId);
		}
        int paramId = BufferUtils.readInt(this.content);
        int dataLen = this.packetLength - 6;
        long pTargetData = Bytes.alloc(pstmt.getHeap(), dataLen);
        long pSourceData = this.content.memoryAddress() + this.content.readerIndex();
        Unsafe.copyMemory(pSourceData, pTargetData + 4, dataLen);
        pstmt.setParam(paramId, pTargetData);
    }
}
