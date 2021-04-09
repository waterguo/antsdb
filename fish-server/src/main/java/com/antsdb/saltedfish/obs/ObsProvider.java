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

import java.io.IOException;
import java.util.List;

public interface ObsProvider {

    void close() throws IOException;

    void createDirectory(String directory) throws Exception;

    void deleteDirectory(String directory) throws Exception;

    boolean existDirectory(String directory) throws Exception; 

    void uploadFile(String key, String fileName, long fsize) throws Exception;

    void deleteObject(String key) throws Exception;

    boolean doesObjectExist(String key) throws Exception;

    void downloadObject(String key, String filename) throws Exception;

    /**
     * 显示指定下路径下的所有文件，如目录下没有文件返回 null 或 empty list
     * @param tablePath
     * @param prefix
     * @param suffix
     * @return
     * @throws Exception
     */
    List<String> listFiles(String tablePath, String prefix,String suffix) throws Exception;

    /**
     * 显示指定下路径下的所有子目录，如目录下没有子目录返回null 或 empty list
     * @param dbname
     * @return
     * @throws Exception
     */
    List<String> listDirectorys(String dbname) throws Exception;
    /**
     * check obs root resource is exist ,example: hdfs base directory,s3 bucket 
     * @param rootResource
     * @return
     * @throws Exception
     */
    boolean checkRootResourceExists(String rootResource) throws Exception;

    byte[] getObjectContentByKey(String syncObjKey) throws Exception;

    Object getName();
    Object getTotalSize();
    
    Object getApiCount();

    Object getApiReadCount();

    Object getApiWriteCount();

    Object getApiListCount();

    Object getTotalUploads();

    Object getTotalUploadsSize();

    Object getTotalDownloads();

    Object getTotalDownloadsSize();
    
    boolean isEmpty(String destBucket);

    void backup(String destBucket) throws Exception;

}
