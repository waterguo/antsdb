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
package com.antsdb.saltedfish.sql.vdm;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.antsdb.saltedfish.util.UberUtil;

public class HashMapRecord extends Record {
    Map<Integer, Object> map = new HashMap<>();
    byte[] key;
    
    @Override
    public Object get(int field) {
        return this.map.get(field);
    }

    @Override
    public Record set(int field, Object val) {
        this.map.put(field, val);
        return this;
    }

    @Override
    public byte[] getKey() {
        return this.key;
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public int hashCode() {
        int result = 0;
        for (Object value:this.map.values()) {
            if (value == null) {
                continue;
            }
            int hash;
            if (value instanceof byte[]) {
                hash = 0;
                for (byte i:((byte[])value)) {
                    hash = hash ^ i;
                }
            }
            else {
                hash = value.hashCode(); 
            }
            result = result ^ hash;
        }
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof HashMapRecord)) {
            return false;
        }
        HashMapRecord that = (HashMapRecord)obj;
        if (this.map.size() != that.map.size()) {
            return false;
        }
        Iterator<Map.Entry<Integer, Object>> i = this.map.entrySet().iterator();
        Iterator<Map.Entry<Integer, Object>> j = that.map.entrySet().iterator();
        for (;;) {
            if (!i.hasNext() || !j.hasNext()) {
                break;
            }
            Map.Entry<Integer, Object> ii = i.next();
            Map.Entry<Integer, Object> jj = j.next();
            if (!ii.getKey().equals(jj.getKey())) {
                return false;
            }
            if (ii.getValue() instanceof byte[]) {
                if (jj.getValue() instanceof byte[]) {
                    byte[] iii = (byte[])ii.getValue();
                    byte[] jjj = (byte[])jj.getValue();
                    if (!Arrays.equals(iii, jjj)) {
                        return false;
                    }
                }
                else {
                    return false;
                }
            }
            else {
                if (!UberUtil.safeEqual(ii.getValue(), jj.getValue())) {
                    return false;
                }
            }
        }
        return true;
    }
}
