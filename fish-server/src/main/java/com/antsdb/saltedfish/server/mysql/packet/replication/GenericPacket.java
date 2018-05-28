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
import com.antsdb.saltedfish.util.UberUtil;

import io.netty.buffer.ByteBuf;

/**
 * it's a generic packet representing unhandled event
 * 
 * @author luor5
 *
 */
public class GenericPacket extends ReplicationPacket {
    static Logger _log = UberUtil.getThisLogger();

    public GenericPacket(int type, long length, long pos) {
        super(type, length, pos);
    }

    @Override
    public void read(MysqlClientHandler handler, ByteBuf in) {
		// ignore packet info
		if (_log.isTraceEnabled())
		{
			// event length - header length is body size
	        byte[] bytes = new byte[(int)eventlength - 19];
	        in.readBytes(bytes);
	        String dump = '\n' + UberUtil.hexDump(bytes);
	        _log.trace(dump);
		}
    }

}
