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

public class ActionDeleteFile implements UploadAction{
    static Logger _logSyncLog = LoggerFactory.getLogger(UploadAction.class.getName() + ".sync-log");
    private final String  objectKey;

    public ActionDeleteFile(String objectKey) {
       this.objectKey = objectKey;
    }
     
    public String getObjectKey() {
        return objectKey;
    }

    @Override
    public boolean doAction(
            File localHome, 
            String remoteHome,
            ObsProvider provider,ObsCache obsCache) throws Exception {
        obsCache.remove(objectKey);
        String remoteObjectKey = remoteHome + this.objectKey;
        if(_logSyncLog.isTraceEnabled()) {
            _logSyncLog.trace("ActionDeleteFile delete object={}",
                    remoteObjectKey/*,provider.doesObjectExist(remoteObjectKey)*/);
        }
        provider.deleteObject(remoteObjectKey);
        return true;
    }
    
    @Override
    public boolean equals(Object obj) {
        if(!(obj instanceof ActionDeleteFile) ) {
            return false;
        }
        return (objectKey.equals(((ActionDeleteFile)obj).getObjectKey()));    
     } 
}
