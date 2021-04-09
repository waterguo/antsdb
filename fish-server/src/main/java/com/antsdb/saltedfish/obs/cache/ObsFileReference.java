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
package com.antsdb.saltedfish.obs.cache;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.antsdb.saltedfish.parquet.ParquetReplicationHandler;
import com.antsdb.saltedfish.util.UberTime;

/**
 * 
 * @author *-xguo0<@
 */
public class ObsFileReference {
    static Logger _logSyncLog = LoggerFactory.getLogger(ParquetReplicationHandler.class.getName() + ".sync-log");
    private volatile File file;
    private volatile long lastRead;
    private String key;
    private long createTime;
    private String source;
    private volatile boolean isDownloaded;

    public ObsFileReference(String key, File file,String source) {
        this.key = key;
        this.file = file;
        this.createTime = UberTime.getTime();
        this.source = source;
    }

    public long getLastRead() {
        return lastRead;
    }

    public void setLastRead(long lastRead) {
        this.lastRead = lastRead;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setFile(File file) {
        this.file = file;
    }
    
    public File getFile() {
        return file;
    }

    public long getFsize() {
        if(file==null) {
            return -1;
        }
        return file.length();
    } 

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    @Override
    public void finalize() {
        _logSyncLog.trace("finalize delete key={} file={}",this.getKey(),this.file);
        this.file.delete();
    }
    
    public void release() {
        if(file!=null) {
            _logSyncLog.trace("release delete key={} file={}",this.getKey(),this.file);
            this.file.delete();
        }
    }

    public boolean isDownloaded() {
        return isDownloaded;
    }

    public void setDownloaded(boolean isDownloaded) {
        this.isDownloaded = isDownloaded;
    }

}
