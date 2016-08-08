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

import java.util.Arrays;

import org.apache.commons.io.Charsets;
import org.slf4j.Logger;

import static com.antsdb.saltedfish.server.mysql.MysqlConstant.*;

import com.antsdb.saltedfish.server.mysql.MysqlServerHandler;
import com.antsdb.saltedfish.server.mysql.util.BufferUtils;
import com.antsdb.saltedfish.util.UberUtil;

import io.netty.buffer.ByteBuf;

public class AuthPacket extends RecievePacket {
    public int clientParam;
    int maxThreeBytes;
    public String user;
    String rawPwd;
    public String dbname;
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
            user = BufferUtils.readString(in);
            if ((clientParam & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0) {
                int passwordLength = in.readByte();
                byte[] bytes = new byte[passwordLength];
                in.readBytes(bytes);
                rawPwd = new String(bytes, Charsets.ISO_8859_1);
            }
            else {
	            if ((clientParam & CLIENT_SECURE_CONNECTION) != 0) {
	                int passwordLength = in.readByte();
	                byte[] bytes = new byte[passwordLength];
	                in.readBytes(bytes);
	                rawPwd = new String(bytes, Charsets.ISO_8859_1);
	            }
	            else {
	            	rawPwd = BufferUtils.readString(in);
	            }
            }
            if ((clientParam & MysqlServerHandler.CLIENT_CONNECT_WITH_DB) != 0) {
            	dbname = BufferUtils.readString(in);
            }
            if ((clientParam & MysqlServerHandler.CLIENT_PLUGIN_AUTH) != 0) {
            	BufferUtils.readString(in);
            }
    	}
    }
    
    /**
     * Check if response is version 41
     * @param in
     * @return
     */
    private boolean checkResponseVer41(ByteBuf in) 
    {
    	// response V41 has at least 32 bytes
    	if (packetLength>=32)
    	{
    		in.markReaderIndex();
    		byte[] reserved = new byte[23];
    		
    		// 23 bytes from index 9 (+4 head offset) are all zero for V41
    		in.getBytes(9 + 4, reserved, 0, 23);
    	    in.resetReaderIndex();
    		if (Arrays.equals(reserved, new byte[23]))
    		{
    			return true;
    		}
    		
    	}
    	return false;
    }

}
