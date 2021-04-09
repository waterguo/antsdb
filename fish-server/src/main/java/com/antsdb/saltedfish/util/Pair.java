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

/**
 * 
 * @author *-xguo0<@
 */
public class Pair<X, Y> {
    public X x;
    public Y y;
    
    public Pair() {
    }
    
    public Pair(X x, Y y) {
        this.x = x;
        this.y = y;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean equals(Object obj) {
        Pair<X,Y> that = (Pair<X, Y>) obj;
        return UberUtil.safeEqual(this.x, that.x) && UberUtil.safeEqual(this.y, that.y);
    }

    @Override
    public int hashCode() {
        return (x == null ? 0 : x.hashCode()) ^ (y == null ? 0 : y.hashCode());
    }

    @Override
    public String toString() {
        return "(" + String.valueOf(x) + "," + String.valueOf(y) + ")";
    }
}
