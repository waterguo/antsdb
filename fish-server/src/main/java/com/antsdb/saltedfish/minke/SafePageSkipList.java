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

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.antsdb.saltedfish.cpp.KeyBytes;

/**
 * 
 * @author *-xguo0<@
 */
final class SafePageSkipList {
    MinkeSkipList<MinkePage> pages = new MinkeSkipList<>();
    
    static class MyComparator implements Comparator<Object> {
        @Override
        public int compare(Object xx, Object yy) {
            long x = getAddress(xx);
            long y = getAddress(yy);
            if ((x == 0) || (y == 0) || (x == Long.MIN_VALUE) || (y == Long.MIN_VALUE)) {
                return Long.compareUnsigned(x, y);
            }
            return KeyBytes.compare(x, y);
        }
        
        long getAddress(Object val) {
            if (val instanceof Long) {
                return (Long)val;
            }
            else {
                return ((KeyBytes)val).getAddress();
            }
        }
    }

    MinkePage put(MinkePage page) {
        return (MinkePage)this.pages.put(page.getStartKeyPointer(), page);
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    Collection<MinkePage> values() {
        return (Collection<MinkePage>)(Collection)this.pages.values();
    }
    
    void clear() {
        this.pages.clear();
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    Set<Map.Entry<KeyBytes,MinkePage>> entrySet() {
        return (Set<Map.Entry<KeyBytes,MinkePage>>)(Set)this.pages.entrySet();
    }
    
    boolean remove(MinkePage page) {
        return this.pages.remove(page.getStartKeyPointer(), page);
    }

    MinkePage get(long pKey) {
        return (MinkePage)this.pages.get(pKey);
    }

    Entry<KeyBytes, MinkePage> floorEntry(long pKey) {
        return this.pages.floorEntry(pKey);
    }

    Entry<KeyBytes, MinkePage> ceilingEntry(long pKey) {
        return this.pages.ceilingEntry(pKey);
    }

    Entry<KeyBytes, MinkePage> higherEntry(long pKey) {
        return this.pages.higherEntry(pKey);
    }

    Entry<KeyBytes, MinkePage> lowerEntry(long pKey) {
        return this.pages.lowerEntry(pKey);
    }

    int size() {
        return this.pages.size();
    }

    MinkePage getFloorPage(long pKey) {
        Map.Entry<KeyBytes, MinkePage> entry = this.pages.floorEntry(pKey);
        return (entry != null) ? entry.getValue() : null;
    }

    MinkePage getCeilingPage(long pKey) {
        Map.Entry<KeyBytes, MinkePage> entry = this.pages.ceilingEntry(pKey);
        return (entry != null) ? entry.getValue() : null;
    }

    MinkePage getHigherPage(long pKey) {
        Entry<KeyBytes, MinkePage> entry = higherEntry(pKey);
        return (entry != null) ? entry.getValue() : null;
    }

    MinkePage getLowerPage(long pKey) {
        Entry<KeyBytes, MinkePage> entry = lowerEntry(pKey);
        return (entry != null) ? entry.getValue() : null;
    }
}
