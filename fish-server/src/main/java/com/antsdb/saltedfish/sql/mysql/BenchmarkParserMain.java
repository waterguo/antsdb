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
package com.antsdb.saltedfish.sql.mysql;

import java.util.ArrayList;

import org.apache.commons.cli.Options;

import com.antsdb.saltedfish.cpp.BetterCommandLine;
import com.antsdb.saltedfish.util.UberTime;

/**
 * 
 * @author *-xguo0<@
 */
public class BenchmarkParserMain extends BetterCommandLine {

    private int nthreads;
    private long startTime;
    private long endTime;
    private int time;
    private String sql;
    
    private class Worker extends Thread {
        long count = 0;
        
        @Override
        public void run() {
            while (UberTime.getTime() < endTime) {
                MysqlParserFactory.parse(sql);
                count++;
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
        new BenchmarkParserMain().parseAndRun(args);
    }

    @Override
    protected void buildOptions(Options options) {
        options.addOption(null, "threads", true, "number of threads, default 1");
        options.addOption(null, "time", true, "number of secodns to run, default is 10");
    }

    @Override
    protected String getCommandName() {
        return "benchmark-parser <sql>";
    }

    @Override
    protected void run() throws Exception {
        if (this.cmdline.getArgs().length < 1) {
            println("error: sql is missing from command line");
            System.exit(-1);
        }
        this.nthreads = Integer.parseInt(this.cmdline.getOptionValue("threads", "1"));
        this.time = Integer.parseInt(this.cmdline.getOptionValue("time", "10"));
        String sql = this.cmdline.getArgs()[0];
        run0(sql);
    }

    private void run0(String sql) throws InterruptedException {
        this.sql = sql;
        this.startTime = UberTime.getTime();
        this.endTime = this.startTime + this.time * 1000;
        ArrayList<Worker> threads = new ArrayList<>();
        for (int i=0; i<this.nthreads; i++) {
            Worker worker = new Worker();
            worker.start();
            threads.add(worker);
        }
        long count = 0;
        for (Worker worker:threads) {
            worker.join();
            count += worker.count;
        }
        println("threads: %d", this.nthreads);
        println("time: %d", this.time);
        println("total: %d", count);
        println("throughput: %d/s", count / this.time);
    }
}
