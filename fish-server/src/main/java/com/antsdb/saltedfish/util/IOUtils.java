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
package com.antsdb.saltedfish.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
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

public class IOUtils {
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
}

