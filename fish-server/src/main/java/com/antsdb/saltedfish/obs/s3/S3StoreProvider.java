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
package com.antsdb.saltedfish.obs.s3;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.antsdb.saltedfish.obs.BaseObsProvider;
import com.antsdb.saltedfish.obs.ExecutorBackupPool;
import com.antsdb.saltedfish.parquet.OrcaObjectStoreException;
import com.antsdb.saltedfish.parquet.bean.BackupObject;
import com.antsdb.saltedfish.util.UberTime;
import com.antsdb.saltedfish.util.UberUtil;

public class S3StoreProvider extends BaseObsProvider {
    static Logger _log = UberUtil.getThisLogger();

    private AmazonS3 client = null;
    private String bucketName;
    
    public S3StoreProvider(S3Config s3config) {
        ClientConfiguration config = new ClientConfiguration();
        config.setProtocol(Protocol.HTTP);
        config.setMaxConnections(100);
        AWSCredentials awsCredentials = new BasicAWSCredentials(s3config.getAccessKey(), s3config.getSecretKey());
        EndpointConfiguration endpointConfiguration = null;
        if (s3config.getEndpoint() != null && s3config.getEndpoint().length() > 10) {
            endpointConfiguration = new EndpointConfiguration(s3config.getEndpoint(), s3config.getClientRegion());
        }
        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();

        if (s3config.getEndpoint() == null || s3config.getEndpoint().length() <= 10) {
            builder.withRegion(s3config.getClientRegion());
        }
        else {
            builder.withEndpointConfiguration(endpointConfiguration);
        }
        builder.withClientConfiguration(config).withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                .withPathStyleAccessEnabled(true);
        client = builder.build();
        bucketName = s3config.getBucketName();
        
        Region region = RegionUtils.getRegion(s3config.getClientRegion());
        String s3Endpoint = region.getServiceEndpoint(AmazonS3.ENDPOINT_PREFIX);
        _log.info("s3 endpoint:{} bucket={}",s3Endpoint,bucketName);
        
        this.name = bucketName;
    }

    @Override
    public void close() {
        _log.info("close s3 provider");
        if (client != null) {
            client.shutdown();
            client = null;
        }
    }

    @Override
    public void createDirectory(String directory) {
        if(directory== null || directory.length() ==0) {
            return;
        }
        if (!directory.endsWith("/")) {
            directory += "/";
        }
        if (client != null) {
            ObjectMetadata meta = new ObjectMetadata();
            meta.setContentLength(0);
            client.putObject(bucketName, directory, new ByteArrayInputStream(new byte[0]), meta);
            this.apiCount ++;
            this.apiWriteCount ++;
        }
        else {
            throw new OrcaObjectStoreException("create directory {} ,but s3 client not connect",directory)  ;
        }
    }

    @Override
    public void deleteDirectory(String directory) {
        if (!directory.endsWith("/")) {
            directory += "/";
        }
        if (client != null) {
            // 递归删除目叟下的所有内容
            ObjectListing objects = client.listObjects(bucketName, directory);
            for (S3ObjectSummary s3ObjectSummary : objects.getObjectSummaries()) {
                String object = s3ObjectSummary.getKey();
                client.deleteObject(bucketName, object);
                this.apiCount ++;
                this.apiWriteCount ++;
            }
            while (objects.isTruncated()) {
                objects = client.listNextBatchOfObjects(objects);
                for (S3ObjectSummary s3ObjectSummary : objects.getObjectSummaries()) {
                    String object = s3ObjectSummary.getKey();
                    client.deleteObject(bucketName, object);
                    this.apiCount ++;
                    this.apiWriteCount ++;
                }
            }
            client.deleteObject(bucketName, directory);
            this.apiCount ++;
            this.apiWriteCount ++;
        }
        else {
            throw new OrcaObjectStoreException("s3 client not connect") ;
        }
    }

    @Override
    public boolean existDirectory(String directory) {
        if (!directory.endsWith("/")) {
            directory += "/";
        }
        if (client != null) {
            this.apiCount ++;
            this.apiReadCount ++;
            return client.doesObjectExist(bucketName, directory);
        }
        else {
            throw new OrcaObjectStoreException("s3 client not connect") ;
        }
    }

    @Override
    public void uploadFile(String key, String fileName, long fsize) throws Exception {
        if (client != null) {
            long startTime = UberTime.getTime();
            ObjectMetadata meta = new ObjectMetadata();
            File f = new File(fileName);
            if(f.length()!=fsize) {
                _log.warn("{} fsize error: length:{},fsize:{}",fileName,f.length(),fsize);
                fsize = f.length();
            }
            String md5Base64 = getMD5(f);
            meta.setContentMD5(md5Base64);
            if (fsize > 0) {
                meta.setContentLength(fsize);
            }
            meta.setContentDisposition(key);
            try (InputStream is = new FileInputStream(f)) {
                client.putObject(bucketName, key, is, meta);
                this.apiCount ++;
                this.apiWriteCount ++;
                this.totalUploads ++;
                this.totalUploadsSize += fsize;
            }
            long endTime = UberTime.getTime();
            _log.trace("upload object :{} use time:{}",key,(endTime - startTime));
        }
        else {
            throw new OrcaObjectStoreException("s3 client not connect") ;
        }
    }

    @Override
    public void deleteObject(String key) {
        long startTime = UberTime.getTime();
        if (doesObjectExist(key)) {
            _log.trace("object {} exists, exec delete object",key);
            if (client != null) {
                client.deleteObject(bucketName, key);
                this.apiCount ++;
                this.apiWriteCount ++;
            }
            else {
                throw new OrcaObjectStoreException("s3 client not connect") ;
            }
        }
        else {
            _log.trace("object {} not exists,return true",key);
        }
        long endTime = UberTime.getTime();
        _log.trace("delete object :{} use time:{}",key,(endTime - startTime));
    }

    @Override
    public boolean doesObjectExist(String key) {
        if (client != null) {
            this.apiCount ++;
            this.apiWriteCount ++;
            boolean exists = client.doesObjectExist(bucketName, key);
            return exists;
        }
        throw new OrcaObjectStoreException("s3 client not connect") ;
    }

    @Override
    public void downloadObject(String key, String filename) throws Exception {
        if (doesObjectExist(key)) {
            if (client != null) {
                GetObjectRequest request = new GetObjectRequest(bucketName, key);
                File f = new File(filename);
                client.getObject(request, f);
                this.apiCount ++;
                this.apiReadCount ++;
                this.totalDownloads ++;
                this.totalDownloadsSize += f.length();
            }
            else {
                throw new OrcaObjectStoreException("s3 client not connect") ;
            }
        }
        else {
            _log.trace("{} not exists.",key);
        }
    }

    @Override
    public List<String> listFiles(String tablePath, String tableNamePrefix,String extName) {
        List<String> files = new ArrayList<>();
        if(tablePath != null  && tablePath.length() > 0 && !tablePath.endsWith("/")){
            tablePath = tablePath + "/";
        }
        if (client != null) {
            this.apiCount ++;
            this.apiListCount ++;
            ObjectListing objects = client.listObjects(bucketName, tablePath);
            
            for (S3ObjectSummary s3ObjectSummary : objects.getObjectSummaries()) {
                String object = s3ObjectSummary.getKey();
                if(object!=null) {
                    if(tableNamePrefix != null) { 
                        String prefix = tablePath + tableNamePrefix;
                        if( object.startsWith(prefix) 
                                && object.endsWith(extName)) {
                            _log.trace("list files 1 table:{},object:{}",tablePath,object);
                            files.add(object);
                        }
                    }
                    else if(object.endsWith(extName)) {
                        files.add(object);
                    }
                }
            }
            while (objects.isTruncated()) {
                objects = client.listNextBatchOfObjects(objects);
                for (S3ObjectSummary s3ObjectSummary : objects.getObjectSummaries()) {
                    String object = s3ObjectSummary.getKey();
                    if(object!=null) {
                        if(tableNamePrefix != null) { 
                            String prefix = tablePath + tableNamePrefix;
                            if( object.startsWith(prefix) 
                                    && object.endsWith(extName)) {
                                _log.trace("list files 2 table:{},object:{}",tablePath,object);
                                files.add(object);
                            }
                        }
                        else if(object.endsWith(extName)) {
                            _log.trace("list files 3 table:{},object:{}",tablePath,object);
                            files.add(object);
                        }
                    }
                }
            }
        }
        else {
            throw new OrcaObjectStoreException("s3 client not connect") ;
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
            ListObjectsRequest request = new ListObjectsRequest();
            request.withBucketName(bucketName);
            if(parethDir != null && parethDir.length() > 0) {
                request.withPrefix(parethDir);
            }
            request.withDelimiter("/");
            this.apiCount ++;
            this.apiListCount ++;
            ObjectListing objects = client.listObjects(request);
            for (String commonPrefix : objects.getCommonPrefixes()) {
                if (!commonPrefix.startsWith(parethDir) || commonPrefix.equals(parethDir)) {// skip dbname dir
                    continue;
                }
                String subDir = commonPrefix.replace(parethDir, "");
                subDir = subDir.replace("/", "");
                dirs.add(subDir);
            }
            while (objects.isTruncated()) {
                objects = client.listNextBatchOfObjects(objects);
                for (String commonPrefix : objects.getCommonPrefixes()) {
                    if (!commonPrefix.startsWith(parethDir) || commonPrefix.equals(parethDir)) {// skip dbname dir
                        continue;
                    }
                    String subDir = commonPrefix.replace(parethDir, "");
                    subDir = subDir.replace("/", "");
                    dirs.add(subDir);
                }
            }
        }
        else {
            throw new OrcaObjectStoreException("s3 client not connect") ;
        }
        return dirs;
    }

    @Override
    public boolean checkRootResourceExists(String rootResource) {
        if (client != null) {
            this.apiCount ++;
            this.apiReadCount ++;
            return client.doesBucketExistV2(bucketName);
        }
        else {
            throw new OrcaObjectStoreException("s3 client not connect") ;
        }
    }

    @Override
    public byte[] getObjectContentByKey(String objectKey) throws Exception {
        if (client != null) {
            this.apiCount ++;
            this.apiReadCount ++;
            S3Object s3Object = client.getObject(bucketName, objectKey);
            if (s3Object != null) {
                byte[] buf = new byte[1024];
                try (
                        InputStream in = s3Object.getObjectContent();
                        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
                ) {
                    int len = -1;
                    while ((len = in.read(buf)) != -1) {
                        outStream.write(buf, 0, len);
                    }
                    return outStream.toByteArray();
                }
            }
            return null;
        }
        else {
            throw new OrcaObjectStoreException("s3 client not connect") ;
        }
    }
    
    /**
     *  fun get md5 code 
     * @return md5 value
     */
    public static String getMD5(File file) {
        FileInputStream fileInputStream = null;
        try {
            MessageDigest MD5 = MessageDigest.getInstance("MD5");
            fileInputStream = new FileInputStream(file);
            byte[] buffer = new byte[8192];
            int length;
            while ((length = fileInputStream.read(buffer)) != -1) {
                MD5.update(buffer, 0, length);
            }
            byte[] md5Bytes = MD5.digest();
            return Base64.encodeBase64String(md5Bytes);
        }
        catch (Exception e) {
            _log.error(e.getMessage(), e);
            return null;
        }
        finally {
            try {
                if (fileInputStream != null) {
                    fileInputStream.close();
                }
            }
            catch (IOException e) {
                _log.error(e.getMessage(), e);
            }
        }
    }

    @Override
    public Object getTotalSize() {
        return "--";
    }
    
    protected void copyObject(String srcBucket,String srcKey,String destBucket,String destKey) {
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
            ObjectListing objects = client.listObjects(bucketName);
            List<BackupObject> data = new ArrayList<>();
            for (S3ObjectSummary s3ObjectSummary: objects.getObjectSummaries()) {
                String objectKey = s3ObjectSummary.getKey();
                data.add(new BackupObject(this.bucketName,destBucket,objectKey));
                count++;
                if(count%1000 == 0) {
                    runBackup(pool,data);
                    data.clear();
                }
                showBackupInfo(destBucket,count);
            }
            if(data!=null && data.size() >0 ) {
                runBackup(pool,data);
                data.clear();
            }
            while (objects.isTruncated()) {
                objects = client.listNextBatchOfObjects(objects);
                for (S3ObjectSummary s3ObjectSummary: objects.getObjectSummaries()) {
                    String objectKey = s3ObjectSummary.getKey();
                    data.add(new BackupObject(this.bucketName,destBucket,objectKey));
                    count++;
                    if(count%1000 == 0) {
                        runBackup(pool,data);
                        data.clear();
                    }
                    showBackupInfo(destBucket,count);
                }
            }
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
