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
package com.antsdb.saltedfish.obs.hdfs;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.apache.parquet.hadoop.util.HiddenFileFilter;
import org.slf4j.Logger;

import com.antsdb.saltedfish.obs.BaseObsProvider;
import com.antsdb.saltedfish.obs.ExecutorBackupPool;
import com.antsdb.saltedfish.parquet.OrcaObjectStoreException;
import com.antsdb.saltedfish.parquet.ParquetFilter;
import com.antsdb.saltedfish.util.UberTime;
import com.antsdb.saltedfish.util.UberUtil;

public class HdfsProvider extends BaseObsProvider {
    static Logger _log = UberUtil.getThisLogger();

    private FileSystem fileSystem = null;
    private Configuration configuration;
    private String baseDirectory;

    private void getFileSystem() {
        if (this.fileSystem == null) {
            try {
                String uri = configuration.get("fs.default.name");
                String hadoopUser = configuration.get("dfs.user.name");
                if (uri != null && uri.length() > 0) {
                    this.name = uri;
                    this.fileSystem = FileSystem.get(new URI(uri), configuration, hadoopUser);
                }
            }
            catch (Exception e) {
                throw new OrcaObjectStoreException(e);
            }
        }
    }
    
    public HdfsProvider(Configuration configuration,String baseDirectory) {
        this.configuration = configuration;
        this.baseDirectory = baseDirectory;
        getFileSystem();
        _log.info("hdfs endpoint:{},uri={}",baseDirectory,fileSystem.getUri());
    }

    @Override
    public void close() throws IOException {
        if (fileSystem != null) {
            _log.info("close hdfs provider");
            fileSystem.close();
            fileSystem = null;
        }
    }

    @Override
    public void createDirectory(String directory) throws IOException {
        Path path = new Path(this.baseDirectory + directory);
        if (path != null && !existsRemote(path)) {
            this.apiCount ++;
            this.apiWriteCount ++;
            
            fileSystem.mkdirs(path);
        }
        else {
            throw new OrcaObjectStoreException("path is null or path({}) exists.",directory) ;
        }
    }
 

    @Override
    public void deleteDirectory(String directory) throws IOException {
        Path path = new Path(this.baseDirectory + directory);
        if (existsRemote(path)) {
            this.apiCount ++;
            this.apiWriteCount ++;
            
            fileSystem.delete(path, true);
        }
        else {
            throw new OrcaObjectStoreException("path({}) not exists.",directory) ;
        }
    }

   

    @Override
    public boolean existDirectory(String directory) throws IOException {
        this.apiCount ++;
        this.apiReadCount ++;
        return existsRemote(this.baseDirectory + directory);
    }

    @Override
    public void uploadFile(String key, String fileName, long fsize) throws Exception {
        uploadFile(fileName, this.baseDirectory + key);
    }

    @Override
    public void deleteObject(String key) throws IOException {
        Path path = new Path(this.baseDirectory + key);
        if (existsRemote(path)) {
            this.apiCount ++;
            this.apiWriteCount ++;
            fileSystem.delete(path, true);
        }
    }

    @Override
    public boolean doesObjectExist(String key) throws IOException {
        return existsRemote(this.baseDirectory + key);
    }

    @Override
    public void downloadObject(String key, String filename) throws Exception {
        Path src = new Path(this.baseDirectory + key);
        if (existsRemote(src)) {
            File dstFile = new File(filename);
            if (!dstFile.exists() && !dstFile.getParentFile().exists()) {
                dstFile.getParentFile().mkdirs();
            }
            try (
                    FSDataInputStream in = fileSystem.open(src);
                    OutputStream outputStream = new FileOutputStream(new File(filename));
            ) {
                IOUtils.copyBytes(in, outputStream, 1024);
                this.apiCount ++;
                this.totalDownloads ++;
                this.totalDownloadsSize += dstFile.length();
            }
        }
        else {
            _log.trace("{} not exists.",key);
        }
    }

    @Override
    public List<String> listFiles(String tablePath,String prefix,String suffix) throws Exception {
        
        this.apiCount ++;
        this.apiListCount ++;
        Path path = new Path(this.baseDirectory + tablePath);
        FileStatus[] filesStatus = fileSystem.listStatus(path,  new ParquetFilter(prefix,suffix));
        if (filesStatus != null && filesStatus.length > 0) {
            List<String> tableFiles = new ArrayList<>();
            for (FileStatus i : filesStatus) {
                String fileName = tablePath +"/"+ i.getPath().getName();
                tableFiles.add(fileName);
            }
            return tableFiles;
        }
        return null;
    }
    
    @Override
    public List<String> listDirectorys(String parentDir) {
        List<String> tables = new ArrayList<>();
        String dir = this.baseDirectory ;
        if(parentDir!=null) {
            dir += parentDir;
        }
        try {
            if(parentDir!=null && !existsRemote(dir)) {
                return tables;
            }
        }
        catch (IOException e) {
            throw new OrcaObjectStoreException(e);
        }
        if(parentDir == null) {
            parentDir = "./";
        }
        try {
            this.apiCount ++;
            this.apiListCount ++;
            
            FileStatus[] filesStatus = fileSystem.listStatus(new Path(dir), HiddenFileFilter.INSTANCE);
            for (FileStatus i : filesStatus) {
                if(!i.isDirectory()) {
                    continue;
                }
                String fileNameTmp = i.getPath().getName();
                tables.add(fileNameTmp);
            }
        }
        catch (IOException e) {
            throw new OrcaObjectStoreException(e);
        }
        return tables;
    }

    @Override
    public boolean checkRootResourceExists(String rootResource) throws IOException {
        this.apiCount ++;
        this.apiReadCount ++;
        return existsRemote(rootResource);
    }

    @Override
    public byte[] getObjectContentByKey(String objectKey) throws Exception {
        Path path = new Path(this.baseDirectory + objectKey);
        if (existsRemote(path) & !fileSystem.getFileStatus(path).isDirectory()) {
            this.apiCount ++;
            this.apiReadCount ++;
            
            FSDataInputStream inputStream = fileSystem.open(path);
            ByteArrayOutputStream bOut = new ByteArrayOutputStream();
            IOUtils.copyBytes(inputStream, bOut, 1024);
            return bOut.toByteArray();
        }
        return null;
    }
    
    private boolean existsRemote(Path file) throws IOException {
        if (file != null) {
            boolean exists = fileSystem.exists(file);
            return exists;
        }
        return false;
    }
    
    private boolean existsRemote(String fileAbsPathName) throws IOException {
        this.apiCount ++;
        this.apiReadCount ++;
        Path path = new Path(fileAbsPathName);
        return existsRemote(path);
    }

    private boolean uploadFile(String srcLocalFile, String hdfsPath) throws IOException {
        File f = new File(srcLocalFile);
        if (f.isFile()) {
            this.apiCount ++;
            this.totalUploads ++;
            this.totalUploadsSize += f.length();
            Path dst = new Path(hdfsPath);
            fileSystem.copyFromLocalFile(new Path(srcLocalFile), dst);
            return existsRemote(dst);
        }
        else {
            _log.warn("file {} is not available file",srcLocalFile);
        }
        return true;
    }

    @Override
    public Object getTotalSize() {
        try {
            return fileSystem.getStatus().getUsed();
        }
        catch(Exception e) {}
        return "--";
    }
 
    @Override
    public boolean isEmpty(String destDirectory) {
        Path d = new Path(destDirectory);
        FileStatus[] files;
        try {
            files = fileSystem.listStatus(d);
            return (files == null || files.length ==0)?true:false;
        }
        catch (Exception e) {
            throw new OrcaObjectStoreException(e);
        }
    }

    @Override
    public void backup(String destDirectory) throws Exception {
        String src = baseDirectory;
        /*
        try {
            Path srcPath = new Path(src);
            Path destPath = new Path(destDirectory);
            _log.info("hdfs backup src={} dest={}",src,destDirectory);
            String uri = src.replaceAll(" ", "%20");
            FileContext fc = FileContext.getFileContext(new URI(uri), configuration);
            fc.util().copy(srcPath, destPath);
        }
        catch (Exception e) {
            throw new OrcaObjectStoreException(e,"hdfs backup src={} dest={}",src,destDirectory);
        }
        */
        if(destDirectory != null && 
                (!destDirectory.endsWith("/") || !destDirectory.endsWith("\\"))) {
            destDirectory += "/";
        }
        long count = 0;
        ExecutorBackupPool pool = new ExecutorBackupPool(1);
        try {
            List<FileStatus> data = new ArrayList<>();
            RemoteIterator<LocatedFileStatus>  files = fileSystem.listFiles(new Path(src), true);
            if(files == null) {
                return ;
            }
            while(files.hasNext()) {
                FileStatus file = files.next();
                //_log.debug("backup src object is:{}",file.getPath());
                data.add(file);
                count++;
                if(count%1000 == 0) {
                    runBackupHdfs(pool,data,src,destDirectory);
                    data.clear();
                }
                showBackupInfo(destDirectory,count);
            }
            if(data!=null && data.size() >0 ) {
                runBackupHdfs(pool,data,src,destDirectory);
                data.clear();
            }
        }finally {
            pool.shutdown();
        }
        _log.info("backup data to {} end,by time={}",destDirectory,UberTime.getTime());
   
    }
    
    protected void runBackupHdfs(ExecutorBackupPool pool,List<FileStatus> data,String src ,String destDirectory) throws Exception {
        if(data == null || data.size() == 0) {
            return;
        }
        List<Future<Exception>> futures = new ArrayList<>();
        for (FileStatus i:data) {
            futures.add(pool.getPool().submit(()-> {
                try {
                    String srcSu = i.getPath().toString().replace(src,"");
                    String dst = destDirectory + srcSu ;
                    if(i.isDirectory()) {
                        fileSystem.mkdirs(new Path(dst));
                    }
                    else {
                        InputStream in = fileSystem.open(i.getPath());
                        try (OutputStream out = fileSystem.create(new Path(dst))) {
                            IOUtils.copyBytes(in, out, this.configuration, true);
                        } finally {
                            IOUtils.closeStream(in);
                        }
                    }
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
}
