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
package com.antsdb.saltedfish.server.mysql.packet.replication;

import org.slf4j.Logger;

import com.antsdb.saltedfish.server.mysql.MysqlClientHandler;
import com.antsdb.saltedfish.server.mysql.util.BufferUtils;
import com.antsdb.saltedfish.util.UberUtil;

import io.netty.buffer.ByteBuf;

/**
 * https://dev.mysql.com/doc/internals/en/table-map-event.html
 * 
 * @author luor5
 *
 */
public class TableMapPacket extends ReplicationPacket {
    static Logger _log = UberUtil.getThisLogger();

    public long tableId;
    public int flags;
    public String schemaName;
    public String tableName;
    public int colCount;
    public int metaCount;
    public byte[] colTypeDef;
    public byte[] colMetaDef;
    public byte[] nullBitMap;
    
    public TableMapPacket(int type, long length, long pos) {
        super(type, length, pos);
    }

    @Override
    public void read(MysqlClientHandler handler, ByteBuf in) {
    	// header
    	int begin = in.readerIndex();

    	tableId = BufferUtils.readPackedInteger(in, 6);
    	flags = BufferUtils.readInt(in);
    	
    	// payload
    	schemaName = BufferUtils.readStringWithLength(in);
    	// seperated by 0
    	in.readByte();
    	tableName = BufferUtils.readStringWithLength(in);
    	// seperated by 0
    	in.readByte();
    	colCount = (int)BufferUtils.readLength(in);
    	colTypeDef = BufferUtils.readBytes(in, colCount);
    	
    	metaCount = (int)BufferUtils.readLength(in);
    	colMetaDef = BufferUtils.readBytes(in, metaCount);

    	int nullCount = (colCount + 7) / 8;
        nullBitMap = new byte[nullCount];
        in.readBytes(nullBitMap);

        int end = in.readerIndex();
        
		if (_log.isTraceEnabled())
		{
			in.readerIndex(begin);
	        byte[] bytes = new byte[end - begin];
	        in.readBytes(bytes);
	        String dump = '\n' + UberUtil.hexDump(bytes);
	        _log.trace("Packet Info:\n"
	        		+ this.toString()
	        		+ "\nTableMapPacket packet:\n" 
	        		+ dump);
		}

    }

}
