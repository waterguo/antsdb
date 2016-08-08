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
package com.antsdb.saltedfish.nosql;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.StandardOpenOption;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

class WriteAheadLogChannel implements ReadableByteChannel {
    Queue<File> logs = new LinkedList<File>();
    InputStream in;
    FileChannel ch;
    boolean isClosed = false;
    
    public WriteAheadLogChannel(List<File> logs) {
        super();
        this.logs.addAll(logs);
    }

    private FileChannel getChannel() throws IOException {
        if (this.ch != null) {
            return ch;
        }
        else {
            File file = this.logs.poll();
            if (file == null) {
                return null;
            }
            this.ch = FileChannel.open(file.toPath(), StandardOpenOption.READ);
            return this.ch;
        }
    }

    @Override
    public boolean isOpen() {
        return !isClosed;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        for (;;) {
            FileChannel in = getChannel();
            if (in == null) {
                return -1;
            }
            int size = in.read(dst);
            if (size > 0) {
                return size;
            }
            else {
                this.ch.close();
                ch = null;
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (this.ch != null) {
            this.ch.close();
            this.ch = null;
        }
        this.isClosed = true;
    }

}
