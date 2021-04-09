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
        String sql = cbuf.toString().trim();
        if (sql.equalsIgnoreCase("1show create table `test`.`test`")) {
            result = writePackets(0x3c85d, out);
        }
        else if (sql.equalsIgnoreCase("1SHOW FULL FIELDS FROM `test`.`test`")) {
            result = writePackets(0x7e23e, out);
        }
        else if (sql.equalsIgnoreCase("1SHOW TABLE STATUS FROM `test` LIKE 'test'")) {
            result = writePackets(0x7d13e, out);
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
