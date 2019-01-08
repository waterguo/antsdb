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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.apache.commons.io.Charsets;

public final class IOUtils {
    public static DataOutputStream dataOutputStream(FileChannel fc) {
        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(Channels.newOutputStream(fc)));
        return out;
    }
    
    public static FileChannel createFile(File file) throws IOException {
        Path path = file.toPath();
        FileChannel fc = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        return fc;
    }
    
    public static MyDataInputStream dataInputStream(File file) throws FileNotFoundException, IOException {
        InputStream in = new FileInputStream(file);
        return new MyDataInputStream(new BufferedInputStream(in));
    }
    
    /**
     * DataOutputStream can't write string more than 65536 bytes long. this function fixes that
     * @param out
     * @param value
     * @throws IOException 
     */
    public static void writeUtf(DataOutputStream out, String value) throws IOException {
        byte[] bytes = value.getBytes(Charsets.UTF_8);
        writeCompactLong(out, bytes.length);
        out.write(bytes);
    }
    
    public static String readUtf(DataInputStream in) throws IOException {
        long length = readCompactLong(in);
        if (length > Integer.MAX_VALUE) {
            throw new IllegalArgumentException();
        }
        byte[] bytes = new byte[(int)length];
        in.readFully(bytes);
        return new String(bytes, Charsets.UTF_8);
    }
    
    /**
     * takes less bytes to write 62 bits integer
     * @param out
     * @param value
     * @throws IOException 
     */
    public static void writeCompactLong(DataOutputStream out, long value) throws IOException {
        if (value < 0) {
            // fix this later
            throw new IllegalArgumentException();
        }
        int bits = 64 - Long.numberOfLeadingZeros(value);
        int bytes = (bits + 2 - 1) / 8 + 1;
        if (bytes > 8 || bytes < 1) {
            // fix this later
            throw new IllegalArgumentException();
        }
        long temp = (value << 2) | (bytes - 1);
        for (int i=0; i<bytes; i++) {
            out.writeByte((byte)temp);
            temp = temp >> 8;
        }
    }
    
    public static long readCompactLong(DataInputStream in) throws IOException {
        int leader = in.readByte() & 0xff;
        int bytes = leader & 0x3;
        long result = leader >>> 2;
        for (int i=1; i<=bytes; i++) {
            int temp = in.readByte() & 0xff;
            result = result | (temp << (8 * i - 2));
        }
        return result;
    }
}

