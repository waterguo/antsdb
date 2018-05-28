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

import java.util.BitSet;

import org.slf4j.Logger;

import com.antsdb.saltedfish.server.mysql.MysqlClientHandler;
import com.antsdb.saltedfish.server.mysql.util.BufferUtils;
import com.antsdb.saltedfish.util.UberUtil;

import io.netty.buffer.ByteBuf;

/**
 * 
 * https://dev.mysql.com/doc/internals/en/rows-event.html
 * 
 * @author luor5
 *
 */
public class RowsEventV2Packet extends ReplicationPacket {
    static Logger _log = UberUtil.getThisLogger();

    public long tableId;
    public int flags;
    public int extraDataLen;
    public int colCount;
    public BitSet colPresentBitmap;
    // for update
    public BitSet colPresentBitmapAftImg;

    public ByteBuf rawRows;
    
    public RowsEventV2Packet(int type, long length, long pos) {
        super(type, length, pos);
    }

    @Override
    public void read(MysqlClientHandler handler, ByteBuf in) {
    	// header
    	int begin = in.readerIndex();
    	
    	// 6 bytes tabble id
    	tableId = BufferUtils.readPackedInteger(in, 6);
    	// 2 bytes flags
    	flags = BufferUtils.readInt(in);
    	extraDataLen = BufferUtils.readInt(in);
    	
    	// body
    	// number of columns
    	int index = in.readerIndex();
    	colCount = (int)BufferUtils.readLength(in);
    	
        int presentCount = (colCount + 7) / 8;
        byte[] presentBits = new byte[presentCount];
        in.readBytes(presentBits);
        
        colPresentBitmap = BitSet.valueOf(presentBits);

        if (eventType==ReplicationPacket.UPDATE_ROWS_EVENT)
        {
        	byte[] presentUptBits = new byte[presentCount];
            in.readBytes(presentUptBits);
            colPresentBitmapAftImg = BitSet.valueOf(presentUptBits);
        }
        
        int readCnt = in.readerIndex() - index;
        
        // eventlength - event_header_length - post_header_length - rest data length
        rawRows = in.readBytes((int)eventlength - 29 - readCnt);
        
        int end = in.readerIndex();
        
		if (_log.isTraceEnabled())
		{
			in.readerIndex(begin);
	        byte[] bytes = new byte[end - begin];
	        in.readBytes(bytes);
	        String dump = '\n' + UberUtil.hexDump(bytes);
	        _log.trace("Packet Info:\n"
	        		+ this.toString()
	        		+ "\nRowsEventV2Packet packet:\n" 
	        		+ dump);
		}

    }

}

