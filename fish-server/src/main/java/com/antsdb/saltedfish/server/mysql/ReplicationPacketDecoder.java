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

import java.nio.ByteBuffer;
import java.util.List;

import org.slf4j.Logger;

import com.antsdb.saltedfish.server.mysql.packet.replication.GenericPacket;
import com.antsdb.saltedfish.server.mysql.packet.replication.ReplicationPacket;
import com.antsdb.saltedfish.server.mysql.packet.replication.RotatePacket;
import com.antsdb.saltedfish.server.mysql.packet.replication.RowsEventV2Packet;
import com.antsdb.saltedfish.server.mysql.packet.replication.StateIndicator;
import com.antsdb.saltedfish.server.mysql.packet.replication.StopPacket;
import com.antsdb.saltedfish.server.mysql.packet.replication.TableMapPacket;
import com.antsdb.saltedfish.server.mysql.packet.replication.XIDPacket;
import com.antsdb.saltedfish.server.mysql.util.BufferUtils;
import com.antsdb.saltedfish.util.CodingError;
import com.antsdb.saltedfish.util.UberUtil;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

class ReplicationPacketDecoder extends ByteToMessageDecoder{
    
    MysqlClientHandler handler;
    static Logger _log = UberUtil.getThisLogger();
    
    
    private int  state = StateIndicator.INITIAL_STATE;
    
    // Verson 4 event header length
    public static int EVENT_HEADER_LENGTH = 19;

    
    public ReplicationPacketDecoder(MysqlClientHandler handler) {
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
        int size = BufferUtils.readLongInt(in) + 1;
        if (size == (0x00ffffff+1)) {
            return;
        }
        if (!in.isReadable(size)) {
            in.resetReaderIndex();
            return;
        }

        int pos = in.readerIndex(); 
        try {
        	ReplicationPacket packet = readPacket(in, size);
            out.add(packet);
        }
        finally {
            int currentPos = in.readerIndex();
            in.skipBytes(size - (currentPos - pos));
        }
    }

    @SuppressWarnings("unused")
    private ReplicationPacket readPacket(ByteBuf in, int size) {
        // packet sequence number for multiple packets
        
        // byte packetSequence = in.readByte();
        byte seqId = in.readByte();
        
        // handshake
        
        ReplicationPacket packet = null;

        // using state to decide how to handle connecting messages.
        if (state==StateIndicator.INITIAL_STATE) {
            packet = new StateIndicator(StateIndicator.INITIAL_STATE);
            packet.packetLength = size;
            packet.packetId = seqId;
            packet.read(this.handler, in);
            state = StateIndicator.RESPONSED_STATE;
        }
        else if (state==StateIndicator.RESPONSED_STATE) {
            byte header = in.readByte();
            
            if (header == 0)
            {
            	packet = new StateIndicator(StateIndicator.HANDSHAKEN_STATE);
                state = StateIndicator.HANDSHAKEN_STATE;
            }
            else
            {
            	packet = new StateIndicator(StateIndicator.HANDSHAKE_FAIL_STATE);
                state = StateIndicator.HANDSHAKE_FAIL_STATE;
            }
            char[] bytes = new char[size];
            for (int i=0; i<size; i++) {
                int ch = in.getByte(i);
                bytes[i] = (char)ch;
            }
            packet.packetId = seqId;
            packet.packetLength = size;
            packet.read(this.handler, in);
        }
        else if (state==StateIndicator.HANDSHAKEN_STATE) {
        	// expecting response for registered slave
            byte header = in.readByte();
            
            if (header == 0)
            {
            	packet = new StateIndicator(StateIndicator.REGISTERED_STATE);
                state = StateIndicator.REGISTERED_STATE;
            }
            else
            {
            	packet = new StateIndicator(StateIndicator.REGISTER_FAIL_STATE);
                state = StateIndicator.REGISTER_FAIL_STATE;
            }
            packet.packetId = seqId;
            packet.packetLength = size;
            packet.read(this.handler, in);
        }
        else {


    		// binlog stream started with 00 ok-byte
            byte okByte = in.readByte();

            if (okByte==0)
            {
	        	// read event header
	            
	            // timestamp 4 bytes
	            int timeStamp = in.readInt();
	            // event type
	            byte eventType = in.readByte();
	            // server id, 4 bytes
				int serverId = (int)BufferUtils.readLong(in);
	            // event length, 4 bytes
	            long eventLength = BufferUtils.readLong(in);
	            // next position, 4 bytes
	            long nextPosition = BufferUtils.readLong(in);
	            // flags
	            int flags = in.readShort();
	            
	            // events
	            
	            switch (eventType) {
	            case ReplicationPacket.ROTATE_EVENT:
	                packet = new RotatePacket(eventType, eventLength, nextPosition );
	                break;
	            case ReplicationPacket.TABLE_MAP_EVENT:
	                packet = new TableMapPacket(eventType, eventLength, nextPosition);
	                break;
	            case ReplicationPacket.WRITE_ROWS_EVENT:
	            case ReplicationPacket.UPDATE_ROWS_EVENT:
	            case ReplicationPacket.DELETE_ROWS_EVENT:
	                packet = new RowsEventV2Packet(eventType, eventLength, nextPosition);
	                break;
	            case ReplicationPacket.STOP_EVENT:
	                packet = new StopPacket(eventType, eventLength, nextPosition);
	                break;
	            case ReplicationPacket.XID_EVENT:
	                packet = new XIDPacket(eventType, eventLength, nextPosition);
	                break;
	            case ReplicationPacket.QUERY_EVENT:
	            case ReplicationPacket.FORMAT_DESCRIPTION_EVENT:
	            case ReplicationPacket.START_EVENT_V3:
	               	// use GenericPacket to ignore unsupported events for now
	            	packet = new GenericPacket(eventType, eventLength, nextPosition);
	            	break;
	            default:
	            	_log.error("unknown event: " + eventType);
	                throw new CodingError("unknown event: " + eventType);    
	             }
	
	            if (packet!=null)
	            {
	                packet.packetId = seqId;
	            	packet.packetLength = size;
	                packet.read(this.handler, in);
	            }
            }
            else
            {
    	    	ByteBuf pkt = (ByteBuf)in;
    			ByteBuffer bbuf = pkt.nioBuffer();
    			
    			int i = bbuf.remaining();
    			
    	        byte[] bytes = new byte[i];
    	        pkt.readBytes(bytes);
    	        String dump = '\n' + UberUtil.hexDump(bytes);
    	        _log.error("unknown packet: " + dump);
                throw new CodingError("unknown packet");    
            }
        }
        return packet;
    }

}
