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

import java.sql.Date;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * generates a 64 bit globally unique id
 *
 * this is a java implementation of the insatgram id
 *
 * 64 bits is its benefits
 *
 * http://instagram-engineering.tumblr.com/post/10853187575/sharding-ids-at-instagram
 *
 * Created by wguo on 14-11-28.
 */
public class IdGenerator {
    static final long EPOCH = Date.valueOf("2007-02-11").getTime();
    static final AtomicInteger COUNTER = new AtomicInteger();
    static int _clientId = 0;

    public static long getId() {
        long tick = System.currentTimeMillis();
        tick = tick - EPOCH;
        long id = tick << (64-41);
        id |= _clientId << (64-41-13);
        id |= COUNTER.getAndIncrement() % 1024;
        return id;
    }

    public static void setClientId(int clientId) {
        _clientId = clientId;
    }
}