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
package com.antsdb.saltedfish.util;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Benchmarker {
    volatile boolean stop = false;
    
    public static long run(int seconds, int nThreads, Class<? extends BenchmarkWorker> klass) 
    throws Exception {
        // information
        
        System.out.println("start..");
        System.out.println("threads: " + nThreads);
        System.out.println("test class: " + klass.getName());
        
        // start threads
        
        long start = System.currentTimeMillis();
        Benchmarker bench = new Benchmarker();
        ExecutorService service = Executors.newFixedThreadPool(nThreads);
        List<Future<Long>> futures = new ArrayList<>();
        for (int i=0; i<nThreads; i++) {
            BenchmarkWorker call = klass.newInstance();
            call.owner = bench;
            Future<Long> future = service.submit(call);
            futures.add(future);
        }
        
        // wait
        
        Thread.sleep(1000l * seconds);
        
        // shutdown
        
        bench.stop = true;
        long count = 0;
        for (Future<Long> i:futures) {
            long n = i.get();
            count += n;
        }
        service.shutdown();
        
        // summary

        long end = System.currentTimeMillis();
        long elapse = end - start;
        long speed = count * 1000 / elapse;
        System.out.println(String.format("elapse: %s ms", toReadeableInteger(elapse)));
        System.out.println(String.format("total: %s ops", toReadeableInteger(count)));
        System.out.println(String.format("speed: %s ops/s", toReadeableInteger(speed)));
        
        return count;
    }
    
    static String toReadeableInteger(long n) {
        return NumberFormat.getNumberInstance(Locale.US).format(n);
    }
}
