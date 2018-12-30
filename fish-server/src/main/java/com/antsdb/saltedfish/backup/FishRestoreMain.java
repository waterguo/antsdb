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
package com.antsdb.saltedfish.backup;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.commons.cli.Options;

import com.antsdb.saltedfish.cpp.BetterCommandLine;
import com.antsdb.saltedfish.cpp.Value;

/**
 * 
 * @author *-xguo0<@
 */
public class FishRestoreMain extends BetterCommandLine {

    public static void main(String[] args) throws Exception {
        new FishRestoreMain().parseAndRun(args);
    }

    @Override
    protected void buildOptions(Options options) {
        options.addOption(null, "file",  true, "dump file");
    }

    @Override
    protected void run() throws Exception {
        InputStream in;
        if (this.cmdline.hasOption("file")) {
            in = new FileInputStream(this.cmdline.getOptionValue("file"));
        }
        else {
            in = System.in;
        }
        //CountingInputStream in = new CountingInputStream(new BufferedInputStream(System.in));
        run(new BufferedInputStream(in, 1024 * 1024 * 16));
    }
        
    @SuppressWarnings("unused")
    private void run(InputStream in) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        DataInputStream din = new DataInputStream(in);
        BackupFile backup = BackupFile.open(in);
        for (TableBackupInfo table:backup.tables) {
            String fullname = din.readUTF();
            System.out.print(fullname + ":");
            long nRows = 0;
            for (;;) {
                din.readFully(buffer.array(), 0, 4);
                int header = buffer.getInt(0);
                if (header == 0) {
                    break;
                }
                if ((header & 0xff) != (Value.FORMAT_ROW & 0xff)) {
                    String msg = String.format("invalid row found: %x @%x", header, 0);
                    throw new IllegalArgumentException(msg);
                }
                din.readFully(buffer.array(), 4, 4);
                int size = buffer.getInt(4);
                if (size > buffer.capacity()) {
                    buffer = ByteBuffer.allocate(size);
                    buffer.order(ByteOrder.LITTLE_ENDIAN);
                    buffer.putInt(header);
                    buffer.putInt(size);
                }
                din.readFully(buffer.array(), 8, size-8);
                nRows++;
            }
            System.out.println(nRows + " rows");
        }
    }
}
