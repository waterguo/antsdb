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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.codec.Charsets;
import org.apache.commons.io.IOUtils;

import com.google.gson.Gson;

/**
 * 
 * @author *-xguo0<@
 */
public class BackupFile {
    List<TableBackupInfo> tables = new ArrayList<>(); 
    
    public void save(OutputStream out) throws IOException {
        DataOutputStream dout = new DataOutputStream(out);
        writeUtf(dout, toJson());
    }
    
    public static BackupFile open(InputStream in) throws IOException {
        DataInputStream din = new DataInputStream(in);
        BackupFile result = new Gson().fromJson(readUtf(din), BackupFile.class);;
        return result;
    }
    
    public void addTable(TableBackupInfo info) {
        this.tables.add(info);
    }
    
    private static void writeUtf(DataOutputStream out, String value) throws IOException {
        byte[] bytes = value.getBytes(Charsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }
    
    private static String readUtf(DataInputStream in) throws IOException {
        int length = in.readInt();
        byte[] bytes = new byte[length];
        IOUtils.readFully(in, bytes);
        String result = new String(bytes, Charsets.UTF_8);
        return result;
    }
    private String toJson() {
        Gson gson = new Gson();
        String json = gson.toJson(this);
        return json;
    }
}
