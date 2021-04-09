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
package com.antsdb.saltedfish.backup;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;

import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author *-xguo0<@
 */
class RestoreThread extends Thread {
    static final Logger _log = UberUtil.getThisLogger();
    
    private Connection conn;
    @SuppressWarnings("unused")
    private String sql;
    private BlockingQueue<List<Object[]>> queue;
    private PreparedStatement stmt;
    volatile Exception error;
    volatile long count;
    volatile boolean isBusy = false;
    
    RestoreThread(Connection conn, BlockingQueue<List<Object[]>> q) {
        setDaemon(true);
        this.conn = conn;
        this.queue = q;
    }

    @Override
    public void run() {
        try {
            run0();
        }
        catch (InterruptedException x) {}
        catch (Exception x) {
            // empty the queue
            this.error = x;
            while (!this.queue.isEmpty()) {
                this.queue.poll();
            }
        }
    }

    void prepare(String sql) throws SQLException {
        closeStatement();
        this.sql = sql;
        this.stmt = this.conn.prepareStatement(sql);
        this.count = 0;
    }
    
    void closeStatement() throws SQLException {
        if (this.stmt != null) {
            this.stmt.close();
            this.stmt = null;
        }
    }
    
    void close() {
        try {
            closeStatement();
            if (this.conn != null) {
                this.conn.close();
                this.conn = null;
            }
        }
        catch (Exception x) {}
    }
    
    private void run0() throws Exception {
        boolean useAutoCommit = this.conn.getAutoCommit();
        this.count = 0;
        for (;;) {
            List<Object[]> rows = this.queue.take();
            if (rows == RestoreThreadPool.EOF) {
                this.queue.put(rows);
                return;
            }
            this.isBusy = true;
            try {
                for (Object[] row:rows) {
                    for (int i=0; i<row.length; i++) {
                        Object value = row[i];
                        stmt.setObject(i+1, value);
                    }
                    stmt.addBatch();
                }
                stmt.executeBatch();
                if (!useAutoCommit) {
                    this.conn.commit();
                }
                this.count += rows.size();
            }
            finally {
                this.isBusy = false;
            }
        }
    }
}
