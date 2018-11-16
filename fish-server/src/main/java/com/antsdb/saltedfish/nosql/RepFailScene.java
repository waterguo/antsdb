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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * 
 * @author *-xguo0<@
 */
public class RepFailScene {
    public String op;
    public String table;
    public String sql;
    public Map<String, Object> fields = new HashMap<>();
    
    public RepFailScene() {
        
    }
    
    public RepFailScene(String op, String table, String sql, Row row) {
        this.op = op;
        this.table = table;
        this.sql = sql;
        for (int i=0; i<=row.getMaxColumnId(); i++) {
            Object value = row.get(i);
            if (value != null) {
                this.fields.put(String.valueOf(i), value);
            }
        }
    }
    
    @SuppressWarnings("unchecked")
    public static RepFailScene load(File file) throws FileNotFoundException, IOException, ClassNotFoundException {
        RepFailScene result = new RepFailScene();
        try (ObjectInputStream in = new ObjectInputStream(new FileInputStream(file))) {
            result.op = (String)in.readObject();
            result.table = (String)in.readObject();
            result.sql = (String)in.readObject();
            result.fields = (Map<String, Object>)in.readObject();
        }
        return result;
    }
    
    public void save(File file) throws FileNotFoundException, IOException {
        try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(file))) {
            out.writeObject(this.op);
            out.writeObject(this.table);
            out.writeObject(this.sql);
            out.writeObject(this.fields);
        }
    }
    
    public static void main(String[] args) throws Exception {
        File file = new File("test");
        new RepFailScene().save(file);
        System.out.println(RepFailScene.load(file));
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("op: ");
        buf.append(this.op);
        buf.append("\n");
        buf.append("table: ");
        buf.append(this.table);
        buf.append("\n");
        buf.append("sql: ");
        buf.append(this.sql);
        buf.append("\n");
        return buf.toString();
    }
}
