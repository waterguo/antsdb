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
package com.antsdb.saltedfish.obs;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import org.slf4j.Logger;

import com.antsdb.saltedfish.parquet.bean.BackupObject;
import com.antsdb.saltedfish.util.UberTime;
import com.antsdb.saltedfish.util.UberUtil;

public abstract class BaseObsProvider implements ObsProvider{
    static Logger _log = UberUtil.getThisLogger();
    
    protected String name;
    protected long apiCount;
    protected long apiReadCount;
    protected long apiWriteCount;
    protected long apiListCount;
    
    protected long totalUploads;
    protected long totalUploadsSize;
    protected long totalDownloads;
    protected long totalDownloadsSize;
    
    @Override
    public Object getName() {
        return name;
    }
    
    @Override
    public Object getApiCount() {
        return apiCount;
    }

    @Override
    public Object getApiReadCount() {
        return apiReadCount;
    }

    @Override
    public Object getApiWriteCount() {
        return apiWriteCount;
    }

    @Override
    public Object getApiListCount() {
        return apiListCount;
    }

    @Override
    public Object getTotalUploads() {
        return totalUploads;
    }

    @Override
    public Object getTotalUploadsSize() {
        return this.totalUploadsSize;
    }

    @Override
    public Object getTotalDownloads() {
        return this.totalDownloads;
    }

    @Override
    public Object getTotalDownloadsSize() {
        return this.totalDownloadsSize;
    }

    protected void copyObject(String srcBucket,String srcKey,String destBucket,String destKey) {
        
    }
    
    protected void runBackup(ExecutorBackupPool pool,List<BackupObject> data) throws Exception {
        if(data == null || data.size() == 0) {
            return;
        }
        List<Future<Exception>> futures = new ArrayList<>();
        for (BackupObject i:data) {
            futures.add(pool.getPool().submit(()-> {
                try {
                    copyObject(i.srcBucket,
                            i.objectKey,
                            i.destBucket,
                            i.objectKey
                            );
                    return null;
                }
                catch (Exception x) {
                    return x;
                }
            }));
        }
        for (Future<Exception> i:futures) {
            Exception x = i.get();
            if (x != null) throw x;
        }
    }
    
    protected void showBackupInfo(String destBucket,long count) {
        if(count%1000 == 0) {
            _log.debug("backup data to {} running,complate={} time={}",destBucket,count,UberTime.getTime());
        }
    }
}
