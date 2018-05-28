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
package com.antsdb.saltedfish.sql.vdm;

import com.antsdb.saltedfish.util.CodingError;

class JoinedRecord extends Record {
    Record left;
    Record right;
    int nColumnsLeft;
    int size;

    JoinedRecord(Record left, Record right, int size, int nColumnsLeft) {
        this.left = left;
        this.right = right;
        this.nColumnsLeft = nColumnsLeft;
        this.size = size;
    }
    
    @Override
    public Object get(int field) {
        if (field < this.nColumnsLeft) {
            return this.left.get(field);
        }
        else if (this.right == null) {
            return null;
        }
        else {
            return this.right.get(field - this.nColumnsLeft);
        }
    }

    @Override
    public Record set(int field, Object val) {
        // joined record is immutable
        throw new CodingError();
    }

    @Override
    public byte[] getKey() {
        // joined record has no key
        return null;
    }

    public boolean isRightNull() {
        return this.right == null;
    }

    @Override
    public int size() {
        return this.size;
    }
    
    boolean isOuter() {
        return this.right == null;
    }
}
