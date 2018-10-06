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

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;

import org.slf4j.Logger;

import com.antsdb.saltedfish.util.JumpException;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author *-xguo0<@
 */
class FakeResponse {
    final static Logger _log = UberUtil.getThisLogger();
    
    static File _file;
    static StreamLog _slog;
    
    static class Handler implements StreamLogReplayHandler {
        int stream;
        ChannelWriter out;
        
        Handler(ChannelWriter out) {
            this.out = out;
        }
        
        @Override
        public void data(File file, int offset, int streamId, ByteBuffer data) {
            if (stream == 0) {
                this.stream = streamId;
            }
            if (this.stream != streamId) {
                return;
            }
            this.out.write(data);
            if (streamId == this.stream-1) {
                throw new JumpException();
            }
        }
    }
    
    static {
        _file = new File("fake");
        if (_file.exists()) {
            try {
                _slog = new StreamLog(_file, "r");
                _slog.open();
            }
            catch (Exception e) {
            }
        }
        _log.info("fake files is loaded: {}", _file);
    }
    
    static boolean fake(CharBuffer cbuf, ChannelWriter out) {
        if (_slog == null) {
            return false;
        }
        boolean result = false;
        String sql = cbuf.toString();
        if (sql.equalsIgnoreCase("SHOW COLUMNS FROM `test`.`test`")) {
            // result = writePackets(0x2a51, out);
        }
        /*else if (sql.equalsIgnoreCase("SHOW TABLE STATUS LIKE 'test'")) {
            result = writePackets(0x2c71, out);
        }
        else if (sql.equalsIgnoreCase("SHOW CREATE TABLE `test`.`test`")) {
            result = writePackets(0x22c9, out);
        }
        else if (sql.equalsIgnoreCase("SELECT SCHEMA_NAME, DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME FROM information_schema.SCHEMATA")) {
            result = writePackets(0x326, out);
        }
        else if (sql.equalsIgnoreCase("SHOW FULL TABLES WHERE Table_type != 'VIEW'")) {
            result = writePackets(0x1606, out);
        }
        else if (sql.equalsIgnoreCase("SHOW TABLE STATUS")) {
            result = writePackets(0x1733, out);
        }
        */
        else if (sql.equalsIgnoreCase("SELECT * FROM `test`.`test` LIMIT 0,1000")) {
            result = writePackets(0x2836, out);
        }
        if (result) {
            _log.info("faked: {}", sql);
        }
        else {
            _log.info("not faked: {}", sql);
        }
        return result;
    }

    private static boolean writePackets(int offset, ChannelWriter out) {
        try {
            _slog.replay(offset, new Handler(out));
            return true;
        }
        catch (JumpException x) {
            return true;
        }
        catch (Exception x) {
            x.printStackTrace();
            return false;
        }
    }
}
