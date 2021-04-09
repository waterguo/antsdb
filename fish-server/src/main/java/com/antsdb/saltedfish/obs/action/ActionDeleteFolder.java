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
package com.antsdb.saltedfish.obs.action;

import java.io.File;

import org.slf4j.Logger;

import com.antsdb.saltedfish.obs.ObsProvider;
import com.antsdb.saltedfish.obs.cache.ObsCache;
import com.antsdb.saltedfish.util.UberUtil;

public class ActionDeleteFolder implements UploadAction {
    static Logger _log = UberUtil.getThisLogger();

    private String partitionIndex;
    private String objectKey;

    public String getPartitionIndex() {
        return partitionIndex;
    }

    public ActionDeleteFolder(String objectKey, String partitionIndexFile) {
        this.objectKey = objectKey;
        this.partitionIndex = partitionIndexFile;
    }
    
    public ActionDeleteFolder(String objectKey) {
        this.objectKey = objectKey;
        this.partitionIndex = null;
    }

    @Override
    public boolean doAction(File localHome, String remoteHome, ObsProvider provider, ObsCache obsCache)
            throws Exception {
        boolean result = false;
        if (objectKey != null && objectKey.length() > 0) {
            String remoteObjectKey = remoteHome + objectKey;
            provider.deleteDirectory(remoteObjectKey);
            if (getPartitionIndex() != null && getPartitionIndex().length() > 0) {
                String indexFileObjectKey = remoteHome + getPartitionIndex();
                provider.deleteObject(indexFileObjectKey);
                result = true;
                _log.trace("delete info remote PartitionIndex:{},deleteObject:{}", indexFileObjectKey,result);
            }
            else {
                result = true;
            }
        }
        else {
            result = false;
        }
        // clean loacl
        if (result) {
            if (objectKey != null && objectKey.length() > 0) {
                File deleteDir = new File(localHome, objectKey);
                _log.debug("delete info loacl:{},exists:{}", deleteDir.getAbsolutePath(),deleteDir.exists());
                if (deleteDir.exists()) {
                    if(deleteDir.isDirectory()) {
                        result = deleteDirectory(deleteDir);
                        _log.trace("delete directory info loacl:{},\t local result:{}", objectKey, result);
                    }
                    else {
                        result = deleteDir.delete();
                        _log.trace("delete file info loacl:{},\t local result:{}", objectKey, result);
                    }
                }
                
                if (result && getPartitionIndex() != null && getPartitionIndex().length() > 0) {
                    File indexFileObjectKey = new File(localHome, getPartitionIndex());
                    if(indexFileObjectKey.exists()) {
                        result = indexFileObjectKey.delete();
                    }
                    else {
                        result = true;
                    }
                    _log.trace("delete info loacl:{},\t local PartitionIndex result:{}", getPartitionIndex(), result);
                }
                else {
                    result = true;
                }
            }
        }
        return result;
    }
    
    
    private boolean deleteDirectory(File dir){
        if (dir.isDirectory()){
            String[] children = dir.list();
            for (int i=0; i<children.length;i++) {
                boolean success = deleteDirectory(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }
        if (dir.delete()){
            return true;
        }else {
            _log.warn("dir del faild",dir.toString());
        }
        return false;
    }

}
