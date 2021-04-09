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
package com.antsdb.saltedfish.util;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.commons.cli.Options;

import com.antsdb.saltedfish.cpp.BetterCommandLine;

/**
 * 
 * @author *-xguo0<@
 */
public class BinaryLogDumpMain extends BetterCommandLine {

    public static void main(String[] args) throws Exception {
        new BinaryLogDumpMain().parseAndRun(args);
    }
    
    @Override
    protected void buildOptions(Options options) {
        super.buildOptions(options);
    }

    @Override
    protected String getCommandName() {
        return "binary-log-dump <file>";
    }

    @Override
    protected void run() throws Exception {
        File file = new File(this.cmdline.getArgs()[0]);
        BinaryLogReader reader = new BinaryLogReader(file);
        try {
            reader.open();
            SimpleDateFormat format = new SimpleDateFormat("YYMMdd HH:mm:ss.SSS");
            while (reader.next()) {
                String message = reader.getMessage();
                long time = reader.getTime();
                List<Object> args = reader.getArgs();
                StringBuilder buf = new StringBuilder();
                buf.append("@");
                buf.append(String.format("%06x", reader.getOffset()));
                buf.append(' ');
                buf.append(format.format(new Date(time)));
                buf.append(' ');
                buf.append(message);
                buf.append(' ');
                for (Object i:args) {
                    if (i instanceof byte[]) {
                        byte[] bytes = (byte[])i;
                        buf.append(hasControl(bytes) ? BytesUtil.toCompactHex(bytes) : new String(bytes));
                    }
                    else {
                        buf.append(String.valueOf(i));
                    }
                    buf.append(' ');
                }
                println(buf.toString());
            }
        }
        finally {
            reader.close();
        }
    }

    boolean hasControl(byte[] bytes) {
        for (int i=0; i<bytes.length; i++) {
            if (Character.isISOControl(bytes[i])) {
                return true;
            }
        }
        return false;
    }
}
