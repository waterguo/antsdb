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

import java.io.UnsupportedEncodingException;
import java.util.List;

import com.antsdb.saltedfish.server.mysql.packet.AuthPacket;
import com.antsdb.saltedfish.server.mysql.packet.ClosePacket;
import com.antsdb.saltedfish.server.mysql.packet.FieldListPacket;
import com.antsdb.saltedfish.server.mysql.packet.InitPacket;
import com.antsdb.saltedfish.server.mysql.packet.LongDataPacket;
import com.antsdb.saltedfish.server.mysql.packet.MySQLPacket;
import com.antsdb.saltedfish.server.mysql.packet.PingPacket;
import com.antsdb.saltedfish.server.mysql.packet.QueryPacket;
import com.antsdb.saltedfish.server.mysql.packet.RecievePacket;
import com.antsdb.saltedfish.server.mysql.packet.SetOptionPacket;
import com.antsdb.saltedfish.server.mysql.packet.ShutdownPacket;
import com.antsdb.saltedfish.server.mysql.packet.StmtClosePacket;
import com.antsdb.saltedfish.server.mysql.packet.StmtExecutePacket;
import com.antsdb.saltedfish.server.mysql.packet.StmtPreparePacket;
import com.antsdb.saltedfish.server.mysql.util.BufferUtils;
import com.antsdb.saltedfish.util.CodingError;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

class PacketDecoder extends ByteToMessageDecoder{
	static final int MAX_PACKET_SIZE = 0xffffff;  
    static final int COMMAND_HANDSKAE= -1; // mysql doesn't have this code
    
    boolean isHandshaken = false;
    MysqlServerHandler handler;
    ByteBuf largePacket;
    
    public PacketDecoder(MysqlServerHandler handler) {
        super();
        this.handler = handler;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // do we have length field in buffer ?
        
        if (!in.isReadable(4)) {
            return;
        }

        // do we have entire packet in the buffer?
        
        in.markReaderIndex();
        int size = BufferUtils.readLongInt(in);
        int sequence = in.readByte() & 0xff;
        if (size == 0) {
            out.add(new ShutdownPacket());
            return;
        }
        if (!in.isReadable(size)) {
            in.resetReaderIndex();
            return;
        }

        // is very large packet?
        
        this.handler.packetSequence = sequence;
        if (size == MAX_PACKET_SIZE) {
        	if (this.largePacket == null) {
        		this.largePacket = ctx.alloc().directBuffer();
        	}
        	this.largePacket.writeBytes(in, MAX_PACKET_SIZE);
        	return;
        }
        if (this.largePacket != null) {
        	this.largePacket.writeBytes(in, size);
        }

        // parse packet
        
    	if (this.largePacket == null) {
            int pos = in.readerIndex(); 
            try {
	            RecievePacket packet = readPacket(in, size);
	            out.add(packet);
            }
            finally {
            	in.readerIndex(pos + size);
            }
    	}
    	else {
            RecievePacket packet = readPacket(this.largePacket, size);
            out.add(packet);
            this.largePacket.release();
            this.largePacket = null;
    	}
    }

    private RecievePacket readPacket(ByteBuf in, int size) {
        // handshake
        
        RecievePacket packet = null;
        try
        {
            if (!this.isHandshaken) {
                this.isHandshaken = true;
                packet = new AuthPacket();
                packet.packetLength = size;
                packet.read(this.handler, in);
            }
            else {
                // command 
                
                byte command = in.readByte();
                size--;
                switch (command) {
                case MySQLPacket.COM_QUERY:
                    packet = new QueryPacket(command);
                    break;
                case MySQLPacket.COM_STMT_PREPARE:
                    packet = new StmtPreparePacket(command);
                    break;
                case MySQLPacket.COM_STMT_EXECUTE:
                    packet = new StmtExecutePacket(command);
                    break;
                case MySQLPacket.COM_PING:
                    packet = new PingPacket(command);
                    break;
                case MySQLPacket.COM_INIT_DB:
                    // Handle init_db cmd as query. Conversion is in QueryPacket read()
                    packet = new InitPacket(command);
                    break;
                case MySQLPacket.COM_STMT_CLOSE:
                    packet = new StmtClosePacket(command);
                    break;
                case MySQLPacket.COM_QUIT:
                    packet = new ClosePacket(command);
                    break;
                case MySQLPacket.COM_STMT_SEND_LONG_DATA:
                    packet = new LongDataPacket(command);
                    break;
                case MySQLPacket.COM_FIELD_LIST:
                    packet = new FieldListPacket(command);
                    break;
                case MySQLPacket.COM_SET_OPTION:
                    packet = new SetOptionPacket(command);
                    break;
                case MySQLPacket.COM_STMT_RESET:
                default:
                    throw new CodingError("unknown command: " + command);    
                }
                packet.packetLength = size;
                packet.read(this.handler, in);
            }
        }
        catch (UnsupportedEncodingException e)
        {
            throw new CodingError("unknown command: " + e);    
        }
        return packet;
    }

}
