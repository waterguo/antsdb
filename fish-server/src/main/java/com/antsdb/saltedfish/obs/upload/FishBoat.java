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
package com.antsdb.saltedfish.obs.upload;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;

import com.antsdb.saltedfish.obs.ObsProvider;
import com.antsdb.saltedfish.obs.action.UploadAction;
import com.antsdb.saltedfish.obs.action.UploadSet;
import com.antsdb.saltedfish.obs.cache.ObsCache;
import com.antsdb.saltedfish.parquet.CheckPoint;
import com.antsdb.saltedfish.parquet.bean.SyncParam;
import com.antsdb.saltedfish.util.UberUtil;

public class FishBoat implements Callable<Boolean> {
    final static Logger _log = UberUtil.getThisLogger();

    public final static String LOCAL_TMP = "----tmp/";
    public final static String REMOTE_TMP = "@----tmp/";

    private ObsProvider provider;
    private UploadSet uploadSet;
    private ExecutorCatchPool catchThreads;
    private File localHome;
    private String remoteHome;
    private long currentSP;
    private CheckPoint checkPoint;
    private ObsCache obsCache;

    public FishBoat(File localHome, 
            String remoteHome, 
            ExecutorCatchPool catchThreads, 
            ObsProvider provider,
            UploadSet uploadSet,
            long currentSP,
            CheckPoint checkPoint,ObsCache obsCache) {
        this.localHome = localHome;
        this.remoteHome = remoteHome;
        this.catchThreads = catchThreads;
        this.provider = provider;
        this.uploadSet = uploadSet;
        this.currentSP = currentSP;
        this.checkPoint = checkPoint;
        this.obsCache = obsCache;
    }
    
    @Override
    public Boolean call() throws Exception {
        for (;;) {
            try {
                upload();
                return true;
            }
            catch (Exception x) {
                _log.warn("failure found in obs upload", x);
                try {
                    Thread.sleep(60000);
                }
                catch (InterruptedException ignored) {}
            }
        }
    }
    
    private void upload() throws Exception {
        uploadUploadSet();
        upload(uploadSet.getUploadActions(),"upload");
        upload(uploadSet.getDeleteActions(),"delete");
        upload(uploadSet.getDropActions(),"drop");
        uploadSyncparam();

        cleanTmp();
        deleteUploadSet();
    }

    private void upload(List<?> actions,String fun) throws Exception {
        if(actions == null || actions.size() == 0) {
            return;
        }
        List<Future<Exception>> futures = new ArrayList<>();
        _log.trace("upload fish {} size:{}",fun, actions.size());
        for (Object i:actions) {
            UploadAction ii = (UploadAction)i;
            futures.add(this.catchThreads.getPool().submit(()-> {
                try {
                    ii.doAction(this.localHome,this.remoteHome,provider,this.obsCache);
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

    private void uploadSyncparam() throws Exception {
        if (uploadSet.getUploadSyncParam() != null){
            uploadSet.getUploadSyncParam().doAction(
                    this.localHome, 
                    this.remoteHome,
                    provider,
                    this.obsCache);
            SyncParam syncParam = uploadSet.getUploadSyncParam().getSyncParam();
            if(this.checkPoint!=null && syncParam.getCurrentSp() > currentSP) {
                _log.debug("updated committed log pointer {} -> {}",currentSP,syncParam.getCurrentSp());
                 this.checkPoint.setLogPointer(syncParam.getCurrentSp());
            }
            else {
                _log.trace("not updated commiter log pointer {} <= {}",currentSP,syncParam.getCurrentSp());
            }
         }
    }

    private void uploadUploadSet() throws Exception {
        File tmpFile = new File(localHome, LOCAL_TMP);
        File uploadSetFile = new File(tmpFile, UploadSet.UPLOADSET_FILENAME);

        String uploadSetContent = UberUtil.toJson(uploadSet);
        FileUtils.writeStringToFile(uploadSetFile, uploadSetContent);

        this.provider.uploadFile(
                remoteHome + REMOTE_TMP + UploadSet.UPLOADSET_FILENAME,
                uploadSetFile.getAbsolutePath(), 
                uploadSetFile.length());
    } 
    
    private void deleteUploadSet() throws Exception {
        this.provider.deleteObject(remoteHome + REMOTE_TMP + UploadSet.UPLOADSET_FILENAME);
    }
    
    private void cleanTmp() throws IOException {
        File tmpFile = new File(localHome, LOCAL_TMP);
        FileUtils.deleteDirectory(tmpFile);
    }
}
