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

import java.nio.ByteBuffer;

import org.slf4j.Logger;

import com.antsdb.saltedfish.server.mysql.MysqlClientHandler;
import com.antsdb.saltedfish.util.UberUtil;

import io.netty.buffer.ByteBuf;

/**
 * it's a fake packet from ReplicationPacketDecoder to MysqlClientHandler, 
 * indication state of replication
 * 
 * @author luor5
 *
 */
public class StateIndicator extends ReplicationPacket {
    static Logger _log = UberUtil.getThisLogger();
    
    enum REPLICATION_STATE {INITIAL, RESPONSED, HANDSHAKEN, REGISTERED, STARTED, STOPPED}
    
    // use minus value to avoid conflict with real event number
    public static int INITIAL_STATE = -1;
    public static int RESPONSED_STATE = -2;
    public static int HANDSHAKEN_STATE = -3;
    public static int HANDSHAKE_FAIL_STATE = -4;
    public static int REGISTERED_STATE = -5;
    public static int REGISTER_FAIL_STATE = -6;
    public static int STARTED_STATE = -7;
    public static int STOPPED_STATE = -8;
    
    public StateIndicator(int state) {
        super(state);
    }

    @Override
    public void read(MysqlClientHandler handler, ByteBuf in) {
		// ignore packet info, assure master is mysql 5.6
		if (_log.isTraceEnabled())
		{
	    	ByteBuf packet = (ByteBuf)in;
			ByteBuffer bbuf = packet.nioBuffer();
			
			int i = bbuf.remaining();
			
	        byte[] bytes = new byte[i];
	        packet.readBytes(bytes);
	        String dump = '\n' + UberUtil.hexDump(bytes);
	        _log.trace(dump);
		}
    }

}
