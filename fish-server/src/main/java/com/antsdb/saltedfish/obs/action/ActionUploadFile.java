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
import org.slf4j.LoggerFactory;

import com.antsdb.saltedfish.obs.ObsProvider;
import com.antsdb.saltedfish.obs.cache.ObsCache;
import com.antsdb.saltedfish.obs.cache.ObsFileReference;

public class ActionUploadFile implements UploadAction {
    static Logger _logSyncLog = LoggerFactory.getLogger(UploadAction.class.getName() + ".sync-log");
    
    private ObsFileReference uploadFile;
    private boolean md5Flag;

    public ActionUploadFile(ObsFileReference uploadFile,boolean md5Flag) {
        this.md5Flag = md5Flag;
        this.uploadFile = uploadFile;
    }

    public ObsFileReference getUploadFile() {
        return uploadFile;
    }

    @Override
    public boolean doAction(File localHome,
            String remoteHome,
            ObsProvider provider, 
            ObsCache obsCache) throws Exception {
         
        File dataFile = uploadFile.getFile();
        if(dataFile==null || !dataFile.exists()) {
            return true;
        }
        String filePath = dataFile.getPath();
        
        provider.uploadFile(
                remoteHome + uploadFile.getKey(), 
                filePath,
                uploadFile.getFsize());
        String md5 = "";
        if(md5Flag) {
            md5 = Md5CaculateUtil.getMD5(dataFile);
        }
        if(_logSyncLog.isTraceEnabled()) {
            _logSyncLog.trace("ActionUploadFile {},size:{},md5:{},upload success",
                    filePath,
                    uploadFile.getFsize(),
                    md5);
        }
        return true;
    }
    
    @Override
    public boolean equals(Object obj) {
        if(!(obj instanceof ActionUploadFile) ) {
            return false;
        }
        return (uploadFile.getKey().equals(((ActionUploadFile)obj).getUploadFile().getKey()));    
     } 
}
