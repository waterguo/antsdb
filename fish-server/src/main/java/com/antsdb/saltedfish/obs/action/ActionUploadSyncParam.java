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

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;

import com.antsdb.saltedfish.obs.ObsProvider;
import com.antsdb.saltedfish.obs.cache.ObsCache;
import com.antsdb.saltedfish.parquet.bean.SyncParam;
import com.antsdb.saltedfish.util.UberUtil;

public class ActionUploadSyncParam implements UploadAction {
    static Logger _log = UberUtil.getThisLogger();
    
    
    private boolean upload;
    private String objectKey;
    private SyncParam syncParam;

    public ActionUploadSyncParam(boolean isUpload,String objectKey,SyncParam syncParam) 
            throws CloneNotSupportedException {
        this.upload = isUpload;
        this.objectKey = objectKey;
        this.syncParam = syncParam.clone();
    }
 
    public String getObjectKey() {
        return objectKey;
    }

    public SyncParam getSyncParam() {
        return syncParam;
    }
    
    public boolean isUpload() {
        return upload;
    }

    @Override
    public boolean doAction(File localHome,String remoteHome,ObsProvider provider, ObsCache obsCache) throws Exception {
        boolean result = true;
        
        if(isUpload()) {
            File file = new File(localHome,getObjectKey());
            String data = UberUtil.toJson(syncParam);
            FileUtils.writeStringToFile(file, data);
            
            provider.uploadFile(
                    remoteHome + getObjectKey(), 
                    file.getAbsolutePath(),
                    file.length());
            _log.trace("upload syncparam to obs success");
        }
        else {
            _log.trace("upload syncparam to obs skip({})...",isUpload());
        }
        
        return result;
    }
}
