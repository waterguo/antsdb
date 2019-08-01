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
package com.antsdb.saltedfish.nosql;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.antsdb.saltedfish.util.UberTime;

/**
 * 
 * @author *-xguo0<@
 */
public class JdbcLog implements Closeable {
    private File file;
    private ObjectOutputStream out;
    private ObjectInputStream in;
    
    public static class Item implements Serializable {
        private static final long serialVersionUID = 1L;
        
        public int result;
        public String sql;
        public Object[] args;
        public long timestamp;
    }
    
    private static class MyOutputStream extends ObjectOutputStream {
        public MyOutputStream(OutputStream out) throws IOException {
            super(out);
        }
        
        @Override
        protected void writeStreamHeader() throws IOException {
          // do not write a header, but reset:
          // this line added after another question
          // showed a problem with the original
          reset();
        }
    }
    
    public JdbcLog(File file) {
        this.file = file;
    }
    
    public void open(boolean read) throws FileNotFoundException, IOException {
        if (read) {
            this.in = new ObjectInputStream(new BufferedInputStream(new FileInputStream(file)));
        }
        else {
            if (this.file.exists() && this.file.length() > 0) {
                FileOutputStream fout = new FileOutputStream(file, true);
                this.out = new MyOutputStream(new BufferedOutputStream(fout));
            }
            else {
                FileOutputStream fout = new FileOutputStream(file, true);
                this.out = new ObjectOutputStream(new BufferedOutputStream(fout));
            }
        }
    }
    
    public void write(int result, String sql, Object[] args) throws IOException {
        Item item = new Item();
        item.result = result;
        item.sql = sql;
        item.args = args;
        item.timestamp = UberTime.getTime();
        this.out.writeObject(item);
        this.out.flush();
    }
    
    public Item read() throws ClassNotFoundException, IOException {
        try {
            Item result = (Item)this.in.readObject();
            return result;
        }
        catch (EOFException ignored) {
            return null;
        }
    }
    
    public List<Item> readAll() throws ClassNotFoundException, IOException {
        List<Item> result = new ArrayList<>();
        for (;;) {
            Item item = read();
            if (item == null) break;
            result.add(item);
        }
        return result;
    }
    
    @Override
    public void close() throws IOException {
        if (this.out != null) this.out.close();
        if (this.in != null) this.in.close();
    }
}
