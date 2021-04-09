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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.apache.parquet.hadoop.util.HiddenFileFilter;

import com.antsdb.saltedfish.parquet.OrcaObjectStoreException;

public class HDFSToolsUtils {
    private FileSystem fileSystem = null;
    private Configuration configuration = null;

    public Configuration getConfiguration() {
        return configuration;
    }

    private HDFSToolsUtils(Configuration configuration) {
        this.configuration = configuration;
        getFileSystem();
    }

    public void close() throws IOException {
        if (fileSystem != null) {
            fileSystem.close();
        }
    }

    private void getFileSystem() {
        if (fileSystem == null) {
            try {
                String uri = configuration.get("fs.default.name");
                String hadoopUser = configuration.get("dfs.user.name");
                if (uri != null && uri.length() > 0) {
                    fileSystem = FileSystem.get(new URI(uri), configuration, hadoopUser);
                }
            }
            catch (IOException e) {
                throw new OrcaObjectStoreException(e);
            }
            catch (Exception e) {
                throw new OrcaObjectStoreException(e);
            }
        }
    }

    public long getFileSizeRemote(String fileAbsPathName) {
        try {
            Path file = new Path(fileAbsPathName);
            return getFileSizeRemote(file);
        }
        catch (Exception e) {
            throw new OrcaObjectStoreException(e);
        }
    }

    public long getFileSizeRemote(Path file) {
        try {
            if (existsRemote(file)) {
                return fileSystem.getContentSummary(file).getLength();
            }
            return -1;
        }
        catch (Exception e) {
            throw new OrcaObjectStoreException(e);
        }
    }

    /**
     *  check file exists
     * @param fileName
     * @return
     * @throws IOException
     */
    public boolean existsRemote(String fileAbsPathName) throws IOException {
        Path path = new Path(fileAbsPathName);
        return existsRemote(path);
    }

    public boolean existsRemote(Path file) throws IOException {
        if (file != null) {
            boolean exists = fileSystem.exists(file);
            return exists;
        }
        return false;
    }

    public boolean isDirectoryRemote(String fileAbsPathName) throws IOException {
        Path path = new Path(fileAbsPathName);
        return isDirectoryRemote(path);
    }

    public boolean isDirectoryRemote(Path path) throws IOException {
        if (path != null) {
            return existsRemote(path) && isDirectory(path);
        }
        return false;
    }

    public boolean mkdirsRemote(String fileAbsPathName) throws IOException {
        Path path = new Path(fileAbsPathName);
        return mkdirsRemote(path);
    }

    public boolean mkdirsRemote(Path path) throws IOException {
        if (path != null && !existsRemote(path)) {
            return fileSystem.mkdirs(path);
        }
        return false;
    }

    // 在hdfs 上创建一个新的文件，将某些数据写入到hdfs中
    public boolean createFileRemote(String fileName, String content) throws IOException {
        Path path = new Path(fileName);
        if (!existsRemote(path)) {
            FSDataOutputStream outputStream = null;
            try {
                outputStream = fileSystem.create(path);
                outputStream.writeUTF(content);
                return true;
            }
            finally {
                if (outputStream != null) {
                    outputStream.flush();
                    outputStream.close();
                }
            }
        }
        return false;
    }

    public boolean createFileRemote(String fileName, byte[] content) throws IOException {
        Path path = new Path(fileName);
        try (
                FSDataOutputStream outputStream = fileSystem.create(path);
        ) {
            outputStream.write(content);
            outputStream.flush();
            return true;
        }
    }

    /**
     * read file by hdfs
     * @param fileName
     * @return
     * @throws IOException
     */
    public String readFileRemote(String fileName) throws IOException {
        Path path = new Path(fileName);
        if (existsRemote(path) & !isDirectory(path)) {
            FSDataInputStream inputStream = fileSystem.open(path);
            String content = inputStream.readUTF();
            return content;
        }
        return null;
    }

    public boolean deleteDirRemote(String filePath) throws IOException {
        Path path = new Path(filePath);
        return deleteDirRemote(path);
    }

    public boolean deleteDirRemote(Path filePath) throws IOException {
        if (existsRemote(filePath)) {
            return fileSystem.delete(filePath, true);
        }
        return false;
    }

    // 删除hdfs 上已有的文件
    public boolean deleteFileRemote(String absFileName) throws IOException {
        Path path = new Path(absFileName);
        if (existsRemote(path)) {
            return fileSystem.delete(path, true);
        }
        return false;
    }

    // 把windows本地的文件上传到hdfs上
    public boolean uploadFile(String srcLocalFile, String hdfsPath) throws IOException {
        if (new File(srcLocalFile).isFile()) {
            Path dst = new Path(hdfsPath);
            fileSystem.copyFromLocalFile(new Path(srcLocalFile), dst);
            return true;
        }
        return false;
    }

    public boolean uploadFileWithProgress(String srcLocalFile, String hdfsPath) throws IOException {
        Path src = new Path(srcLocalFile);
        if (existsRemote(src)) {
            try (
                    InputStream in = new BufferedInputStream(new FileInputStream(new File(srcLocalFile)));
                    FSDataOutputStream outputStream = fileSystem.create(new Path(hdfsPath), new Progressable() {
                        public void progress() {
                            // 进度条的输出
                            System.out.print("#");
                        }
                    });
            ) {
                IOUtils.copyBytes(in, outputStream, 4096);
            }

        }
        return false;
    }

    // 把hdfs的文件下载到windows上
    /**
     * fun 
     * @param fileName
     * @param localPath
     * @param delSrcFile
     * @return
     * @throws Exception
     */
    public boolean downloadFile(String fileName, String localPath, boolean delSrcFile) throws Exception {
        Path src = new Path(fileName);
        Path dst = new Path(localPath);
        if (existsRemote(src)) {
            // 第一个false参数表示不删除源文件，第4个true参数表示使用本地原文件系统，因为这个Demo程序是在Windows系统下运行的。
            fileSystem.copyToLocalFile(delSrcFile, src, dst, true);
            return true;
        }
        return false;
    }

    public boolean downloadFile(String hdfsFileName, String localFilePath) throws Exception {
        Path src = new Path(hdfsFileName);
        if (existsRemote(src)) {
            File dstFile = new File(localFilePath);
            if (!dstFile.exists() && !dstFile.getParentFile().exists()) {
                dstFile.getParentFile().mkdirs();
            }
            try (
                    FSDataInputStream in = fileSystem.open(src);
                    OutputStream outputStream = new FileOutputStream(new File(localFilePath));
            ) {
                IOUtils.copyBytes(in, outputStream, 1024);
                return true;
            }
        }
        return false;
    }

    // 从本机上传文件到hdfs，采用读写的方式

    public void uploadFile2(String fileName, String hdfsPath) throws IOException {

        Path path = new Path(hdfsPath);
        if (!existsRemote(path)) {
            FileInputStream inputStream = new FileInputStream(fileName);
            FSDataOutputStream dataOutputStream = fileSystem.create(path);
            byte[] bytes = new byte[5];
            int length = 0;
            while ((length = inputStream.read(bytes)) != -1) {
                dataOutputStream.write(bytes, 0, length);
                dataOutputStream.flush();
            }
            dataOutputStream.close();
            inputStream.close();
        }
    }

    // 查看hdfs某个文件的状态
    public FileStatus[] getFileStatus(String fileName) throws FileNotFoundException, IOException {
        Path path = new Path(fileName);
        FileStatus[] status = getFileStatus(path, HiddenFileFilter.INSTANCE);
        return status;

    }

    public FileStatus[] getFileStatus(String fileName, PathFilter filter) throws FileNotFoundException, IOException {
        Path path = new Path(fileName);
        FileStatus[] status = getFileStatus(path, filter);
        return status;

    }

    public List<String> getFilesByPath(Path path, PathFilter filter) throws FileNotFoundException, IOException {
        FileStatus[] filesStatus = fileSystem.listStatus(path, filter);
        if (filesStatus != null && filesStatus.length > 0) {
            List<String> tableFiles = new ArrayList<>();
            for (FileStatus i : filesStatus) {
                String fileName = i.getPath().toString();// .getName();
                tableFiles.add(fileName);
            }
            return tableFiles;
        }
        return null;
    }

    public FileStatus[] getFileStatus(Path path, PathFilter filter) throws FileNotFoundException, IOException {
        FileStatus[] status = fileSystem.listStatus(path, filter);
        return status;

    }

    // 给一个目录的路径，递归的列出该目录下面所有的文件的状态信息(不包括文件夹信息)方式1
    public void getALLFileStatus(String fileName, boolean recursion) throws Exception {
        Path path = new Path(fileName);
        FileStatus[] status = fileSystem.listStatus(path);
        if (recursion) {
            for (FileStatus fileStatus : status) {
                if (fileStatus.isDirectory()) {
                    getALLFileStatus(fileStatus.getPath().toString(), recursion);
                }
                else {
                    System.out.println(fileStatus);
                }
            }
        }
    }

    // 给一个目录的路径，递归的列出该目录下面所有的文件的状态信息(不包括文件夹信息)方式2
    public void getALLFileStatus2(String fileName) throws Exception {

        Path path = new Path(fileName);
        if (isDirectory(path)) {
            FileStatus[] status = fileSystem.listStatus(path);
            for (FileStatus fileStatus : status) {
                getALLFileStatus2(fileStatus.getPath().toString());
            }
        }
        else {
            FileStatus s1 = fileSystem.getFileLinkStatus(path);
            System.out.println(s1);
        }
    }

    /**
     * upload file to hdfs
     * @param localPath
     * @param hdfsPath
     * @throws IOException
     */
    public void uploadToHdfs(String localPath, String hdfsPath) throws IOException {

        Path path = new Path(hdfsPath);
        if (!existsRemote(path)) {
            FSDataOutputStream outputStream = fileSystem.create(path);
            outputStream.close();
        }
        FileReader fr = new FileReader(localPath);
        FSDataOutputStream fsout = fileSystem.append(path);
        char[] chars = new char[5];
        while (fr.read(chars) != -1) {
            fsout.writeUTF(String.valueOf(chars));
        }
        fr.close();
        fsout.close();
    }

    /**
     * down file by hdfs to local
     * @param hdfsPath
     * @param localPath
     * @throws IOException
     */
    public void downToLocal(String hdfsPath, String localPath) throws IOException {
        Path path = new Path(hdfsPath);
        FSDataInputStream fsinput = fileSystem.open(path);
        String content = fsinput.readUTF();
        FileWriter fw = new FileWriter(localPath);
        fw.write(content);
        fw.close();
        fsinput.close();
    }

    /**
     *fun 汇总运算结果，将多个输出合并成一个流文件
     * 
     * @param resultPath
     * @return
     * @throws IOException
     */
    public InputStream getResultFromHDFS(String resultPath) throws IOException {
        SequenceInputStream sis = null;
        if (resultPath != null) {
            FileStatus[] files = fileSystem.listStatus(new Path(resultPath));
            if (files != null) {
                Vector<InputStream> vector = new Vector<InputStream>();
                for (FileStatus fs : files) {
                    Path filePath = fs.getPath();
                    if (filePath.getName().startsWith("part")) {
                        System.out.println("name : " + filePath.getName());
                        InputStream ins = fileSystem.open(filePath);
                        vector.add(ins);
                    }
                }
                Enumeration<InputStream> enumer = vector.elements();
                sis = new SequenceInputStream(enumer);
            }
        }
        return sis;
    }

    

    public boolean copyFile(String srcPath, String dstPath) {
        try {
            FSDataInputStream inputStream = fileSystem.open(new Path(srcPath));
            FSDataOutputStream dataOutputStream = fileSystem.create(new Path(dstPath));
            byte[] bytes = new byte[5];
            int length = 0;
            while ((length = inputStream.read(bytes)) != -1) {
                dataOutputStream.write(bytes, 0, length);
                dataOutputStream.flush();
            }
            dataOutputStream.close();
            inputStream.close();
            return true;
        }
        catch (Exception e) {
            throw new OrcaObjectStoreException(e);
        }
    }
     
    public String copyToDir(String srcPath, String dstPath) throws IOException {
        fileSystem.copyFromLocalFile(false, true, new Path(srcPath), new Path(dstPath));
        return dstPath;
    }
    
    private boolean isDirectory(Path path) throws IOException {
        FileStatus fileStatus = fileSystem.getFileStatus(path);
        if(fileStatus!=null) {
            return fileStatus.isDirectory();
        }
        return false;
    }
}
