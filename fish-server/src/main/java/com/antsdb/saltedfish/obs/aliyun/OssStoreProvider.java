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
package com.antsdb.saltedfish.obs.aliyun;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.common.auth.CredentialsProvider;
import com.aliyun.oss.common.auth.DefaultCredentialProvider;
import com.aliyun.oss.model.CopyObjectRequest;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.PutObjectResult;
import com.antsdb.saltedfish.obs.BaseObsProvider;
import com.antsdb.saltedfish.obs.ExecutorBackupPool;
import com.antsdb.saltedfish.parquet.OrcaObjectStoreException;
import com.antsdb.saltedfish.parquet.bean.BackupObject;
import com.antsdb.saltedfish.util.UberTime;
import com.antsdb.saltedfish.util.UberUtil;

public class OssStoreProvider extends BaseObsProvider {
    static Logger _log = UberUtil.getThisLogger();
    private OSS client;
    private String bucketName;
    
    public OssStoreProvider(OssConfig config) {
        this.bucketName = config.getBucketName();
        this.name = bucketName;
        CredentialsProvider provider = new DefaultCredentialProvider(config.getAccessKey(), config.getSecretKey());
        if(config.getEndpoint()==null || config.getEndpoint().length() == 0) {
            config.setEndpoint(config.getClientRegion() +".aliyuncs.com");
        }
        client =  new OSSClientBuilder().build(config.getEndpoint(), provider);
        _log.info("oss endpoint:{} backet={}",config.getEndpoint(),bucketName);
    }

    @Override
    public void close() {
        if(client!=null) {
            client.shutdown();
        }
        _log.info("close aliyun oss provider");
    }

    @Override
    public void createDirectory(String directory) {
        if (!directory.endsWith("/")) {
            directory += "/";
        }
        if (client != null) {
            this.apiCount ++;
            this.apiWriteCount ++;
            
            PutObjectResult result = client.putObject(bucketName, directory, new ByteArrayInputStream(new byte[0]));
            _log.trace("create Directory {}",result.getResponse()) ;
        }
        else {
            throw new OrcaObjectStoreException("oss client not connect") ;
        }
    }

    @Override
    public void deleteDirectory(String directory) {
        if (!directory.endsWith("/")) {
            directory += "/";
        }
        if (client != null) {
            if (existDirectory(directory)) {
                this.apiCount ++;
                this.apiWriteCount ++;
                
                String nextMarker = null;
                ObjectListing objListing;
                do {
                    ListObjectsRequest listObjectsRequest = new ListObjectsRequest(bucketName);
                    if(directory != null) {
                        listObjectsRequest.setPrefix(directory);
                    }
                    
                    if (nextMarker == null) { // 第一次的分页
                        objListing = client.listObjects(listObjectsRequest);
                    }
                    else { // 以后的分页，附带nextMarker
                        listObjectsRequest.withMarker(nextMarker);
                        objListing = client.listObjects(listObjectsRequest);
                    }
                    List<OSSObjectSummary> sums = objListing.getObjectSummaries();
                    for (OSSObjectSummary s : sums) {
                        String ossKey = s.getKey();
                        client.deleteObject(bucketName, ossKey);
                    }
                    nextMarker = objListing.getNextMarker();
                } while (objListing.isTruncated());
                
                client.deleteObject(bucketName, directory);
            }
        }
        else {
            throw new OrcaObjectStoreException("oss client not connect") ;
        }
    }

    @Override
    public boolean existDirectory(String directory) {
        if(directory == null || directory.length() ==0 || directory.equals("/")) {
            return true;
        }
        
        if (!directory.endsWith("/")) {
            directory += "/";
        }
        if (client != null) {
            this.apiCount ++;
            this.apiReadCount ++;
            return client.doesObjectExist(bucketName, directory);
        }
        else {
            throw new OrcaObjectStoreException("oss client not connect") ;
        }
    }

    @Override
    public void uploadFile(String key, String fileName, long fileSize) throws Exception {
        long startTime = UberTime.getTime();
        if (client != null) {
            File f = new File(fileName);
            if(f.length()!=fileSize) {
                _log.warn("{} fileSize error: length:{},fsize:{}",fileName,f.length(),fileSize);
                fileSize = f.length();
            }
            
            this.apiCount ++;
            this.apiWriteCount ++;
            this.totalUploads ++;
            this.totalUploadsSize += fileSize;
            
            InputStream is = new FileInputStream(f);
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(is.available());
            metadata.setHeader("Pragma", "no-cache");
            metadata.setContentEncoding("utf-8");
            metadata.setContentDisposition("filename/filesize=" + fileName + "/" + fileSize + "Byte.");
            client.putObject(bucketName, key, is, metadata);
            long endTime = UberTime.getTime();
            _log.trace("upload object :{} use time:{}",key,(endTime - startTime));
        }
        else {
            throw new OrcaObjectStoreException("oss client not connect") ;
        }
    }

    @Override
    public void deleteObject(String key) {
        long startTime = UberTime.getTime();
        if (client != null) {
            if (doesObjectExist(key)) {
                this.apiCount ++;
                this.apiWriteCount ++;
                
                _log.trace("object {} exists, exec delete object",key);
                client.deleteObject(bucketName,key);
            }
            else {
                _log.trace("object {} not exists,return true",key);
            }
            long endTime = UberTime.getTime();
            _log.trace("delete object :{} use time:{}",key,(endTime - startTime));
        }
        else {
            throw new OrcaObjectStoreException("oss client not connect") ;
        }
    }

    @Override
    public boolean doesObjectExist(String key) {
        this.apiCount ++;
        this.apiReadCount ++;
        return client.doesObjectExist(bucketName,key);
    }

    @Override
    public void downloadObject(String key, String filename) throws Exception {
        if (client != null) {
            if (doesObjectExist(key)) {
                 
                File file = new File(filename);
                this.apiCount ++;
                this.apiWriteCount ++;
                this.totalDownloads ++;
                this.totalDownloadsSize += file.length();
                
                if (!file.getParentFile().exists()){
                    file.getParentFile().mkdirs();
                }
                client.getObject(new GetObjectRequest(bucketName, key), new File(filename));
            }
            else {
                _log.trace("{} not exists.",key);
            }
        }
        else {
            throw new OrcaObjectStoreException("oss client not connect") ;
        }
    }

    @Override
    public List<String> listFiles(String tablePath, String tbname,String extName) {
        List<String> files = new ArrayList<>();
        if (!tablePath.endsWith("/")) {
            tablePath += "/";
        }
        if (client != null) {
            this.apiCount ++;
            this.apiListCount ++;
            
            String nextMarker = null;
            ObjectListing objListing;
            do {
                ListObjectsRequest listObjectsRequest = new ListObjectsRequest(bucketName);
                if(tablePath!=null) {
                    listObjectsRequest.setPrefix(tablePath);
                }
                
                if (nextMarker == null) { // 第一次的分页
                    objListing = client.listObjects(listObjectsRequest);
                }
                else { // 以后的分页，附带nextMarker
                    listObjectsRequest.withMarker(nextMarker);
                    objListing = client.listObjects(listObjectsRequest);
                }
                List<OSSObjectSummary> sums = objListing.getObjectSummaries();
                for (OSSObjectSummary objectSummary : sums) {
                    String object = objectSummary.getKey();
                    if(object!=null) {
                        if(tbname != null) { 
                            String prefix = tablePath + tbname;
                            if( object.startsWith(prefix) 
                                    && object.endsWith(extName)) {
                                files.add(object);
                            }
                        }
                        else if(object.endsWith(extName)) {
                            files.add(object);
                        }
                    }
                }
                nextMarker = objListing.getNextMarker();
            } while (objListing.isTruncated());
        }
        else {
            throw new OrcaObjectStoreException("oss client not connect") ;
        }
        return files;
    }

    @Override
    public List<String> listDirectorys(String parethDir) {
        List<String> dirs = new ArrayList<>();
        if(parethDir != null  && parethDir.length() > 0 && !parethDir.endsWith("/")){
            parethDir = parethDir + "/";
        }
        if(parethDir !=null  && parethDir.length() > 0 && !doesObjectExist(parethDir)) {
            return dirs;
        }
        if (client != null) {
            this.apiCount ++;
            this.apiListCount ++;
            
            String nextMarker = null;
            ObjectListing objListing;
            do {
                ListObjectsRequest listObjectsRequest = new ListObjectsRequest(bucketName);
                if(parethDir != null && parethDir.length() > 0) {
                    listObjectsRequest.withPrefix(parethDir);
                }
                listObjectsRequest.withDelimiter("/");
                if (nextMarker == null) { // 第一次的分页
                    objListing = client.listObjects(listObjectsRequest);
                }
                else { // 以后的分页，附带nextMarker
                    listObjectsRequest.withMarker(nextMarker);
                    objListing = client.listObjects(listObjectsRequest);
                }
                List<String> sums = objListing.getCommonPrefixes();
                for (String commonPrefix : sums) {
                    if (!commonPrefix.startsWith(parethDir) || commonPrefix.equals(parethDir)) {// skip dbname dir
                        continue;
                    }
                    String subDir = commonPrefix.replace(parethDir, "");
                    subDir = subDir.replace("/", "");
                    dirs.add(subDir);
                }
                nextMarker = objListing.getNextMarker();
            } while (objListing.isTruncated());
        }
        else {
            throw new OrcaObjectStoreException("oss client not connect") ;
        }
        return dirs;
    }

    @Override
    public boolean checkRootResourceExists(String rootResource) {
        if (client != null) {
            this.apiCount ++;
            this.apiReadCount ++;
            return client.doesBucketExist(this.bucketName);
        }
        else {
            throw new OrcaObjectStoreException("oss client not connect") ;
        }
    }

    @Override
    public byte[] getObjectContentByKey(String objectKey) throws Exception {
        if (client != null) {
            this.apiCount ++;
            this.apiReadCount ++;
            OSSObject object = client.getObject(bucketName, objectKey);
            try (InputStream is = object.getObjectContent(); 
                    ByteArrayOutputStream output = new ByteArrayOutputStream();) {
                byte[] buffer = new byte[1024];
                int len = 0;
                while ((len = is.read(buffer)) != -1) {
                    output.write(buffer, 0, len);
                }
                return output.toByteArray();
            }
        }
        else {
            throw new OrcaObjectStoreException("oss client not connect") ;
        }
    }

    @Override
    public Object getTotalSize() {
        return "--";
    }

    protected void copyObject(String srcBucket, String srcKey, String destBucket, String destKey) {
        if(srcBucket == null || srcBucket.length() ==0
                || destBucket == null || destBucket.length() ==0
                || srcKey == null || srcKey.length() ==0
                || destKey == null || destKey.length() ==0
                ) {
            throw new OrcaObjectStoreException("param error") ;
        }
        if(srcBucket.equals(destBucket)
                && srcKey.equals(destKey)) {
            throw new OrcaObjectStoreException("Source and target buckets and key are the same") ;
        }
        CopyObjectRequest copyObjectRequest = new CopyObjectRequest(srcBucket, srcKey,
                destBucket, destKey);
        client.copyObject(copyObjectRequest);        
    }

    @Override
    public boolean isEmpty(String destBucket) {
        ObjectListing lists = client.listObjects(destBucket);
        return (lists==null || lists.getObjectSummaries().size()==0)?true:false;
    }

    @Override
    public void backup(String destBucket) throws Exception {
        _log.info("backup data to {} start,by time={}",destBucket,UberTime.getTime());
        long count = 0;
        ExecutorBackupPool pool = new ExecutorBackupPool(4);
        try {
            String nextMarker = null;
            ObjectListing objListing;
            List<BackupObject> data = new ArrayList<>();
            do {
                ListObjectsRequest listObjectsRequest = new ListObjectsRequest(bucketName);
                
                if (nextMarker == null) { // 第一次的分页
                    objListing = client.listObjects(listObjectsRequest);
                }
                else { // 以后的分页，附带nextMarker
                    listObjectsRequest.withMarker(nextMarker);
                    objListing = client.listObjects(listObjectsRequest);
                }
                List<OSSObjectSummary> sums = objListing.getObjectSummaries();
                for (OSSObjectSummary objectSummary : sums) {
                    String objectKey = objectSummary.getKey();
                    data.add(new BackupObject(this.bucketName,destBucket,objectKey));
                    count++;
                    if(count%1000 == 0) {
                        runBackup(pool,data);
                        data.clear();
                    }
                    showBackupInfo(destBucket,count);
                }
                nextMarker = objListing.getNextMarker();
            } while (objListing.isTruncated());
            if(data!=null && data.size() >0 ) {
                runBackup(pool,data);
                data.clear();
            }
        }finally {
            pool.shutdown();
        }
        _log.info("backup data to {} end,by time={}",destBucket,UberTime.getTime());

    }
   
}
