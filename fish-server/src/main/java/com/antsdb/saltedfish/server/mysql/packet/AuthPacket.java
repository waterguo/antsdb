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

import java.nio.charset.Charset;

import org.slf4j.Logger;

import com.antsdb.saltedfish.server.mysql.MysqlServerHandler;
import com.antsdb.saltedfish.server.mysql.util.BufferUtils;
import com.antsdb.saltedfish.util.CodingError;
import com.antsdb.saltedfish.util.UberUtil;

import io.netty.buffer.ByteBuf;

public class AuthPacket extends RecievePacket {
    public int clientParam;
    int maxThreeBytes;
    public String user;
    public String rawPwd;
    public String dbname;
    // ssl request packet if true 
    public boolean isSSL = false;
    String plugin;
    static Logger _log = UberUtil.getThisLogger();
    
    public AuthPacket() {
        super(-1);
    }

    @Override
    public void read(MysqlServerHandler handler, ByteBuf in) {
    	if (!checkResponseVer41(in)) {
            clientParam = BufferUtils.readInt(in);
            maxThreeBytes = BufferUtils.readLongInt(in);
            user = BufferUtils.readString(in);
            rawPwd = BufferUtils.readString(in);
            dbname = BufferUtils.readString(in);
    	}
    	else {
            clientParam = BufferUtils.readLong(in);
            maxThreeBytes = BufferUtils.readLong(in);
    		in.readByte(); // charset
    		in.skipBytes(23); // reserved for future

            if ((clientParam & MysqlServerHandler.CLIENT_SSL) != 0 && !handler.sslConnected) {
            	// flag this packet as SSL request and handler will establish SSL connection
            	isSSL = true;
            	return;
        	}
            else {
            	// process as handshake packet
                try {
                	user = BufferUtils.readString(in);
                    if ((clientParam & MysqlServerHandler.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA ) != 0){
		                int passwordLength = (int)BufferUtils.readLength(in);
		                // there's null at the end of string
		                if (passwordLength>0)
		                {
			                byte[] bytes = new byte[passwordLength];
			                in.readBytes(bytes);
			                int len = passwordLength;
			                if (bytes[passwordLength-1]==0) {
			                	len = passwordLength-1;
			                };
		                	rawPwd = new String(bytes, 0, len, Charset.defaultCharset());
		                }
		            } else if ((clientParam & MysqlServerHandler.CLIENT_SECURE_CONNECTION ) != 0){
		                int passwordLength = BufferUtils.readByte(in);
		                // there's null at the end of string
		                if (passwordLength>0)
		                {
			                byte[] bytes = new byte[passwordLength];
			                in.readBytes(bytes);
			                int len = passwordLength;
			                if (bytes[passwordLength-1]==0) {
			                	len = passwordLength-1;
			                };
		                	rawPwd = new String(bytes, 0, len, Charset.defaultCharset());
		                }
		            }
		            else
		            {
		            	rawPwd = BufferUtils.readString(in);
		            }
	                
                }
                catch (Exception e)
                {
                	throw new CodingError("Failed to parse client packet, please verify client options: " + e);
                }
            }

            if ((clientParam & MysqlServerHandler.CLIENT_CONNECT_WITH_DB) != 0) {
            	dbname = BufferUtils.readString(in);
            }
            
            if ((clientParam & MysqlServerHandler.CLIENT_PLUGIN_AUTH) != 0) {
            	plugin = BufferUtils.readString(in);
            }
    	}
    	handler.isHandshaken = true;
    }
    
    /**
     * Check if response is version 41
     * @param in
     * @return
     */
    private boolean checkResponseVer41(ByteBuf in) 
    {
    	boolean is41 = false;
		in.markReaderIndex();
		int reserved;
		
		reserved = BufferUtils.readInt(in);
		
		// check PROTOCOL_$! flag
		if ((reserved & MysqlServerHandler.CLIENT_PROTOCOL_41) != 0)
		{
			is41 = true;
		}
		
	    in.resetReaderIndex();
		
    	return is41;
    }

}
