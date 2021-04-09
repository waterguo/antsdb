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
package com.antsdb.saltedfish.obs.s3;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;

import com.antsdb.saltedfish.obs.ObsProvider;
import com.antsdb.saltedfish.util.UberUtil;

public class BenchmarkUploadThread implements Callable<Boolean> {
    protected static Logger _log = UberUtil.getThisLogger();
    private ObsProvider client;

    final String fileName;
    final String key;
    final long fsize;
    final CountDownLatch begin;
    final CountDownLatch end;

    public BenchmarkUploadThread(ObsProvider client, String key, String fileName, long fsize, CountDownLatch begin,
            CountDownLatch end) {
        this.client = client;
        this.key = key;
        this.fileName = fileName;
        this.fsize = fsize;
        this.begin = begin;
        this.end = end;
    }

    @Override
    public Boolean call() throws Exception {
        try {
            begin.await();
            client.uploadFile(key, fileName, fsize);
            return true;
        }
        catch (Throwable e) {
            _log.error(e.getMessage(), e);
        }
        finally {
            end.countDown();
        }
        return false;
    }
}
