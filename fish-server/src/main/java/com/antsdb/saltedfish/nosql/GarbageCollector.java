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
package com.antsdb.saltedfish.nosql;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;

import com.antsdb.saltedfish.util.UberTime;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class GarbageCollector {
    static Logger _log = UberUtil.getThisLogger();
    
    ConcurrentLinkedQueue<Item> junks = new ConcurrentLinkedQueue<>();
    
    static class Item {
        long ts;
        Object obj;
    }
    
    public synchronized void collect(long ts) {
        List<Item> list = new ArrayList<Item>();
        for (Item i:this.junks) {
            if (i.ts < ts) {
                list.add(i);
            }
        }
        for (Item i:list) {
            if (i.obj instanceof Closeable) {
                try {
                    ((Closeable)i.obj).close();
                }
                catch (IOException e) {
                    _log.warn("unable to collect garbabe {}", i.obj, e);
                    continue;
                }
            }
            if (i.obj instanceof Recycable) {
                try {
                    ((Recycable)i.obj).recycle();
                }
                catch (Exception e) {
                    _log.warn("unable to recycle {}", i.obj, e);
                    continue;
                }
            }
            this.junks.remove(i);
        }
    }
    
    public void free(Object obj) {
        Item item = new Item();
        item.ts = UberTime.getTime();
        item.obj = obj;
        this.junks.add(item);
    }
}
