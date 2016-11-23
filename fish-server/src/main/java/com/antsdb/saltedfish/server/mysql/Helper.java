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
package com.antsdb.saltedfish.server.mysql;

import com.antsdb.saltedfish.sql.vdm.Cursor;
import com.antsdb.saltedfish.sql.vdm.CursorMeta;
import com.antsdb.saltedfish.sql.vdm.FieldMeta;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

/**
 * 
 * @author wgu0
 */
class Helper {
	static void writeCursor(ByteBuf bufferArray, MysqlServerHandler serverHandler, Cursor result, boolean text) {
        try (Cursor cursor = (Cursor) result) {
        	int nColumns = getColumnCount(cursor.getMetadata());
            PacketEncoder.writePacket(
                    bufferArray, 
                    serverHandler.getNextPacketSequence(), 
                    () -> serverHandler.packetEncoder.writeResultSetHeaderBody(
                    		bufferArray, 
                    		nColumns));

            // write parameter field packet
    
            for (int i=0; i<nColumns; i++) {
            	FieldMeta column = cursor.getMetadata().getColumn(i);
                PacketEncoder.writePacket(
                        bufferArray, 
                        serverHandler.getNextPacketSequence(), 
                        () -> serverHandler.packetEncoder.writeColumnDefBody(bufferArray, column));
            }
    
            PacketEncoder.writePacket(
                    bufferArray, 
                    serverHandler.getNextPacketSequence(), 
                    () -> serverHandler.packetEncoder.writeEOFBody(bufferArray, serverHandler.getSession()));
    
            // output row
            for (;;) {
                long pRecord = cursor.next();
                if (pRecord == 0) {
                    break;
                }
                if (text) {
                    PacketEncoder.writePacket(
                            bufferArray, 
                            serverHandler.getNextPacketSequence(), 
                            () -> serverHandler.packetEncoder.writeRowTextBody(
                            		bufferArray, 
                            		pRecord, 
                            		nColumns));
                }
                else {
                    PacketEncoder.writePacket(
                            bufferArray, 
                            serverHandler.getNextPacketSequence(), 
                            () -> serverHandler.packetEncoder.writeRowBinaryBody(
                            		bufferArray, 
                            		pRecord, 
                            		cursor.getMetadata(), 
                            		nColumns));
                }
            }
    
            // end row
            PacketEncoder.writePacket(
                    bufferArray, 
                    serverHandler.getNextPacketSequence(), 
                    () -> serverHandler.packetEncoder.writeEOFBody(bufferArray, serverHandler.getSession()));
        }
	}

    private static int getColumnCount(CursorMeta metadata) {
    	// skip system columns , the ones starts with "*"
    	int j = 0;
    	for (FieldMeta i: metadata.getColumns()) {
    		if (i.getName().startsWith("*")) {
    			break;
    		}
    		j++;
    	}
		return j;
	}

	static void writeResonpse(ChannelHandlerContext ctx, MysqlServerHandler handler, Object result, boolean text) {
        ByteBuf bufferArray = ctx.alloc().buffer();
        
        if (result==null){
            PacketEncoder.writePacket(
                    bufferArray, 
                    (byte)1, 
                    () -> handler.packetEncoder.writeOKBody(
                    		bufferArray, 
                    		0, 
                    		handler.session.getLastInsertId(),
                    		null,
                    		handler.session));
        }
        else if (result instanceof Cursor) {
        	Helper.writeCursor(bufferArray, handler, (Cursor) result, text);
        }
        else if (result instanceof Integer) {
            Integer count = (Integer) result;
            PacketEncoder.writePacket(
                    bufferArray, 
                    handler.getNextPacketSequence(), 
                    () -> handler.packetEncoder.writeOKBody(
                    		bufferArray, 
                    		count, 
                    		handler.session.getLastInsertId(),
                    		null,
                    		handler.session));
        }
        else {
            bufferArray.writeBytes(PacketEncoder.OK_PACKET);
        }
        // write preparedOk packet
        ctx.writeAndFlush(bufferArray);
	}

}
