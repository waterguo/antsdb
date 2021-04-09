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
package com.antsdb.saltedfish.minke;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.nosql.TableType;

/**
 * 
 * @author *-xguo0<@
 */
class PageIndexFile {
    static final int EOF_MARK = 0x77777777;
    static final int VERSION = 2;
    
    File file;
    
    static interface LoadCallback {
        void main(long version);
        void table(long version, int tableId, TableType type);
        void page(long version, int pageId, int waste, long lastAccess, KeyBytes startKey, KeyBytes endKey);
    }
    
    PageIndexFile(File file) {
        this.file = file;
    }
    
    PageIndexFile next() {
        String id = file.getName().substring(0, 8);
        int nextId = Integer.parseInt(id, 16) + 1;
        return getFile(this.file.getParentFile(), nextId);
    }
    
    static String getFileName(int id) {
        return String.format("%08x.pif", id);
    }

    static PageIndexFile getFile(File home, int id) {
        return new PageIndexFile(new File(home, getFileName(id)));
    }
    
    /**
     * find the latest page index file from the specified directory
     * 
     * @param folder
     * @return null if not found
     */
    static PageIndexFile find(File folder) {
        List<File> files = new ArrayList<>(Arrays.asList(folder.listFiles((it)->{
            return it.getName().endsWith(".pif");
        })));
        if (files.size() == 0) {
            return null;
        }
        Collections.sort(files);
        return new PageIndexFile(files.get(files.size()-1));
    }
    
    long load(Minke minke, Map<Integer, MinkeTable> tableById) throws Exception {
        try (DataInputStream in = new DataInputStream(new FileInputStream(this.file))) {
            long sp;
            long version = in.readLong();
            if (version >>> 56 == 0xff) {
                version = version & 0xffffl;
                sp = in.readLong();
            }
            else {
                sp = version;
                version = 0;
            }
            for (;;) {
                if (in.available() <= 0) {
                    break;
                }
                int tableId = in.readInt();
                if (tableId == EOF_MARK) {
                    break;
                }
                TableType type = TableType.values()[in.readShort()];
                MinkeTable mtable = new MinkeTable(minke, tableId, type);
                mtable.read(version, in);
                tableById.put(tableId, mtable);
            }
            return sp;
        }
    }

    long load(LoadCallback callback) throws Exception {
        try (DataInputStream in = new DataInputStream(new FileInputStream(this.file))) {
            long sp;
            long version = in.readLong();
            if (version >>> 56 == 0xff) {
                version = version & 0xffffl;
                sp = in.readLong();
            }
            else {
                sp = version;
                version = 0;
            }
            callback.main(version);
            for (;;) {
                int tableId = in.readInt();
                if (tableId == EOF_MARK) {
                    break;
                }
                TableType type = TableType.values()[in.readShort()];
                callback.table(version, tableId, type);
                loadTable(version, in, callback);
            }
            return sp;
        }
    }
    
    private void loadTable(long version, DataInputStream in, LoadCallback callback) throws IOException {
        for (;;) {
            int pageId = in.readInt();
            if (pageId == -1) {
                return;
            }
            long lastAccess = 0;
            if (version >= 1) {
                lastAccess = in.readLong(); 
            }
            int waste = 0;
            if (version >= 2) {
                waste = in.readInt();
            }
            KeyBytes startKey = MinkeTable.readKeyBytes(in);
            KeyBytes endKey = MinkeTable.readKeyBytes(in);
            callback.page(version, pageId, waste, lastAccess, startKey, endKey);
        }
    }

    public void save(Map<Integer, MinkeTable> tableById, long sp) throws IOException {
        File temp = new File(this.file.getAbsolutePath() + ".tmp");
        try (DataOutputStream out = new DataOutputStream(new FileOutputStream(temp))) {
            out.writeLong(VERSION | (0xff << 56));
            out.writeLong(sp);
            for (Map.Entry<Integer, MinkeTable> i:tableById.entrySet()) {
                out.writeInt(i.getKey());
                out.writeShort(i.getValue().getType().ordinal());
                i.getValue().write(out);
            }
            out.writeInt(EOF_MARK);
        }
        temp.renameTo(this.file);
    }
}
