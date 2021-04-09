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

import java.util.HashMap;
import java.util.Map;

import com.antsdb.saltedfish.nosql.ConfigService;
import com.antsdb.saltedfish.nosql.StorageEngine;
import com.antsdb.saltedfish.obs.cache.ObsCache;
import com.antsdb.saltedfish.parquet.CheckPoint;
import com.antsdb.saltedfish.parquet.ObsService;
import com.antsdb.saltedfish.sql.PropertyBasedView;
import com.antsdb.saltedfish.sql.vdm.VdmContext;

/**
 * provides obs store information as a system view - antsdb.obs_info
 * 
 * OBS_TYPE: short class name of the ObsProvider
 * OBS_LP: current log pointer
 * OBS_COMMITED_LP: committed log pointer
 * OBS_SIZE: total number of bytes used in the obs store
 * OBS_NAME: bucket name or HDFS root directory name
 * TOTAL_UPLOADS: total number of uploads
 * TOTAL_UPLOAD_SIZE: total number of bytes of all uploaded files
 * TOTAL_DOWNLOADS: total number of downloads
 * TOTAL_DOWNLOAD_SIZE: total number of bytes all downloaded files
 * CACHE_HIT_RATIO: obs cache hit ratio
 * CACHE_USAGE: number of bytes used by the files in the obs cache
 * CACHE_SIZE: number of bytes specified by the obs cache configuration
 * CACHE_FILE_COUNT: number of files currently in the obs cache
 * API_COUNT: total number of obs api calls
 * API_READ_COUNT: count of READ and SELECT calls
 * API_WRITE_COUNT: count of all write API calls
 * API_LIST_COUNT: count of list calls
 * LATENCY: difference of time between data gets into antsdb and data uploaded to obs
 * STATUS: LIVE or DEAD
 * MERGE_COUNT: number of merges 
 * MERGE_FFICIENCY: total number of new rows divided by the total number of rows in the merge process
 *  
 * @author *-xguo0<@
 */
public class SystemViewObsInfo extends PropertyBasedView {

    private ConfigService config;

    public SystemViewObsInfo(ConfigService config) {
        this.config = config;
    }

    @Override
    public Map<String, Object> getProperties(VdmContext ctx) {
        Map<String, Object> data = new HashMap<>();

        StorageEngine stor = ctx.getHumpback().getStorageEngine0();
        if (stor instanceof ObsService) {
            ObsService service = (ObsService) stor;
            CheckPoint checkPoint = service.getCheckPoint();
            long obsLp = service.getReplicateLogPointer();

            ObsCache cache = service.getObsCache();
            
            ObsProvider provider = service.getObsProvider();
            
            data.put("OBS_TYPE", config.getStorageEngineName());
            data.put("OBS_LP", obsLp);
            data.put("OBS_COMMITED_LP", checkPoint.getCurrentSp());
            
            data.put("OBS_SIZE", provider.getTotalSize());
            data.put("OBS_NAME", provider.getName());
            data.put("TOTAL_UPLOADS", provider.getTotalUploads());
            data.put("TOTAL_UPLOAD_SIZE", provider.getTotalUploadsSize());
            data.put("TOTAL_DOWNLOADS", provider.getTotalDownloads());
            data.put("TOTAL_DOWNLOAD_SIZE", provider.getTotalDownloadsSize());
            
            data.put("CACHE_GET_COUNTS", cache.getGetCounts());
            data.put("CACHE_HIT_COUNTS", cache.getHit());
            data.put("CACHE_HIT_RATIO", cache.getHitRatio());
            data.put("CACHE_USAGE", cache.getUsage());
            data.put("CACHE_SIZE", cache.getMaxSize());
            data.put("CACHE_FILE_COUNT", cache.getCounts());
            
            data.put("API_COUNT", provider.getApiCount());
            data.put("API_READ_COUNT", provider.getApiReadCount());
            data.put("API_WRITE_COUNT", provider.getApiWriteCount());
            data.put("API_LIST_COUNT", provider.getApiListCount());
            data.put("LATENCY", "");
            data.put("STATUS", "");
            data.put("MERGE_COUNT", "");
            data.put("MERGE_FFICIENCY", "");
        }

        return data;
    }

}
