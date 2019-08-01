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
package com.antsdb.saltedfish.beluga;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;

import com.antsdb.saltedfish.slave.DbUtils;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.util.RecentCounter;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class SlaveWarmer extends Thread {
    private static final Logger _log = UberUtil.getThisLogger();
    private static final int MAX_QUEUE_SIE = 20;
    
    long totalError = 0;
    private Pod pod;
    private ConcurrentLinkedQueue<Item> queue = new ConcurrentLinkedQueue<>();
    private HashMap<String, Connection> pool = new HashMap<>();  
    private RecentCounter counter = new RecentCounter(5);

    private class Item {
        String ns;
        String sql;
    }
    
    public SlaveWarmer(Pod pod) {
        this.pod = pod;
        setDaemon(true);
        setName("SlaveWarmer");
    }

    @Override
    public void run() {
        _log.info("slave warmer is started");
        try {
            run0();
        }
        catch (InterruptedException x) {
        }
        catch (Exception x) {
            _log.warn("error", x);
        }
        _log.info("slave warmer is stopped");
    }

    private void run0() throws Exception {
        for (;;) {
            Item item = this.queue.poll();
            if (item == null) {
                Thread.sleep(2000);
                continue;
            }
            for (Member i:this.pod.members) {
                if (!i.warmer) {
                    break;
                }
                try {
                    Connection conn = getConnection(i);
                    if (!StringUtils.isEmpty(item.ns)) {
                        DbUtils.execute(conn, "use " + item.ns);
                    }
                    DbUtils.execute(conn, item.sql);
                    this.counter.count(1);
                }
                catch (SQLException x) {
                    this.totalError++;
                    break;
                }
            }
        }
    }
    
    private Connection getConnection(Member i) throws SQLException {
        Connection conn = this.pool.get(i.endpoint);
        if (conn == null) {
            conn = i.createConnection();
            this.pool.put(i.endpoint, conn);
        }
        else {
            DbUtils.ping(conn);
        }
        return conn;
    }

    public void send(String ns, String sql, Parameters params, Object result) {
        if (!this.pod.warm) {
            return;
        }
        if (sql.length() > 1024) {
            return;
        }
        if ((params != null) && params.size() > 0) {
            return;
        }
        if (this.queue.size() > MAX_QUEUE_SIE) {
            return;
        }
        Item item = new Item();
        item.ns = ns;
        item.sql = sql;
        this.queue.add(item);
    }

    public long getRecentHits() {
        double result = this.counter.getCount();
        return Math.round(result);
    }
    
    public long getTotalHits() {
        return this.counter.getTotal();
    }
}
