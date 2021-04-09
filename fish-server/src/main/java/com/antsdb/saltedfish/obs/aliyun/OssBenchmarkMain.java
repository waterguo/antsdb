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

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import com.antsdb.saltedfish.cpp.BetterCommandLine;
import com.antsdb.saltedfish.nosql.ConfigService;
import com.antsdb.saltedfish.obs.LocalFileUtils;
import com.antsdb.saltedfish.obs.ObsProvider;
import com.antsdb.saltedfish.obs.s3.BenchmarkUploadThread;
import com.antsdb.saltedfish.parquet.OrcaObjectStoreException;
import com.antsdb.saltedfish.parquet.ParquetDataWriter;
import com.antsdb.saltedfish.util.SizeConstants;

/**
 * 
 * @author lizc@tg-hd.com
 */
public class OssBenchmarkMain extends BetterCommandLine {
    CommandLine line;
    private ConfigService config;
    private static MessageType schema;

    private final String TEST_EXT_NAME = ".test";
    private final String S3TABLE_FORMAT = "benchmark_%s_%08d";
    private File home;
    String localData;

    public static void main(String[] args) throws Exception {
        new OssBenchmarkMain().parseAndRun(args);
    }

    @Override
    protected void buildOptions(Options options) {
        options.addOption("h", "help", false, "print help");
    }

    @Override
    protected void run() throws Exception {

        line = this.cmdline;
        if (line.hasOption("wait")) {
            println("press anykey to continue ...");
            System.in.read();
        }
        if (line.getArgList().size() < 3) {
            System.err.println(
                    "Error:args param is not specified, Example S3BenchmarkMain home filesize(M) theradCount runtime(s) ");
            System.exit(-1);
        }
        List<String> params = line.getArgList();

        home = new File(params.get(0));
        int fileSize = Integer.parseInt(params.get(1));
        int threadCount = Integer.parseInt(params.get(2));
        int runtime = Integer.parseInt(params.get(3)) * 1000;

        File fileConfig = new File(home, "conf/conf.properties");
        if (!fileConfig.exists()) {
            println("error: config file is not found: %s", fileConfig);
            System.exit(-1);
        }
        setup();

        // connecting

        println("AntsDB home: %s,Write file size:%s,,Thread %s,Plan run time:%s ", home, fileSize, threadCount,
                runtime);

        File tmp = new File(home, "/tmp");
        if (tmp.exists() && tmp.isDirectory()) {
            File[] fi = tmp.listFiles((file, name) -> name.endsWith(TEST_EXT_NAME));
            for (File f : fi) {
                f.delete();
                println("clean tmp file : %s", f);
            }
        }

        this.config = new ConfigService(fileConfig);
        localData = config.getLocalData();

        String compressCodec = config.getDatafileCompressionCodec();
        if (compressCodec == null || compressCodec.length() == 0) {
            compressCodec = "UNCOMPRESSED";
        }
        CompressionCodecName compressionType = CompressionCodecName.valueOf(compressCodec.toUpperCase());
        String filePath = gendata(fileSize, compressionType);

        OssConfig ossConfig = new OssConfig(config.getOssServiceEndpoint(), 
                config.getOssRegion(), 
                config.getOssAccessKey(),
                config.getOssSecretKey(),
                config.getOssBucketName()
                );
        if (ossConfig.getClientRegion() == null && ossConfig.getEndpoint() == null) {
            println("oss region and endpoint is null,please config");
            throw new OrcaObjectStoreException("oss region and endpoint is null,please config");
        }
        if ( ossConfig.getAccessKey() == null || ossConfig.getSecretKey() == null) {
            println("oss auth info cannot be empty or capitalized");
            throw new OrcaObjectStoreException("s3 auther info cannot be empty or capitalized");
        }
        ObsProvider client = new OssStoreProvider(ossConfig);
        ExecutorService exec = Executors.newFixedThreadPool(threadCount);
        long useTime = 0;
        long filename = 0;
        long fsize = LocalFileUtils.getFileSizeLocal(filePath);
        long sumfileSize = 0;
        long fileCount = 0;

        List<String> uploadfiles = new ArrayList<>();
        long minUseTime = Long.MAX_VALUE;
        long startRunTime = System.currentTimeMillis();

        while (true) {
            final CountDownLatch begin = new CountDownLatch(1);
            final CountDownLatch end = new CountDownLatch(threadCount);
            List<Future<Boolean>> results = new ArrayList<>();
            long startUpladTime = System.currentTimeMillis();
            for (int i = 0; i < threadCount; i++) {
                String key = String.format(S3TABLE_FORMAT, i, filename);
                uploadfiles.add(key);
                sumfileSize += fsize;
                fileCount++;
                BenchmarkUploadThread thread = new BenchmarkUploadThread(client, key, filePath, fsize, begin, end);
                results.add(exec.submit(thread));
                filename++;
            }
            begin.countDown();
            try {
                end.await();
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }

            long endGenTime = System.currentTimeMillis();
            long singleUseTime = endGenTime - startUpladTime;
            if (singleUseTime < minUseTime) {
                minUseTime = singleUseTime;
            }
            println("Upload %sM data use time:%s", fileSize,singleUseTime);
            useTime = (endGenTime - startRunTime);
            if (useTime >= runtime) {
                break;
            }
        }
        exec.shutdown();

        if (tmp.exists() && tmp.isDirectory()) {
            File[] fi = tmp.listFiles((file, name) -> name.endsWith(TEST_EXT_NAME));
            for (File f : fi) {
                f.delete();
                println("clean tmp file : %s", f);
            }
        }

        if (uploadfiles != null && uploadfiles.size() > 0) {

            for (String key : uploadfiles) {
                client.deleteObject(key);
                println("clean test object : %s", key);
            }
        }
        BigDecimal sumdata = BigDecimal.valueOf(sumfileSize).divide(BigDecimal.valueOf(SizeConstants.MB), 3,
                RoundingMode.HALF_UP);
        BigDecimal perSecondUpload = BigDecimal.valueOf(sumfileSize).divide(BigDecimal.valueOf(useTime), 6,
                RoundingMode.HALF_UP);
        BigDecimal perSecondUploadS = perSecondUpload.multiply(BigDecimal.valueOf(1000));
        BigDecimal perSecondUploadMS = perSecondUploadS.divide(BigDecimal.valueOf(SizeConstants.MB), 3,
                RoundingMode.HALF_UP);
        println("Single file size:%s(M),Thread %s,Sum Uplaod data file (%s) %s bytes ,%s(M),use time:%s(ms),"
                + "Throughput:%s(B/MS),%s(B/S),%s(M/S),Min time:%s(ms)", fileSize, threadCount, fileCount, sumfileSize, sumdata,
                useTime, perSecondUpload, perSecondUploadS, perSecondUploadMS,minUseTime);
        println("");

        println("done");
        println("s3 benchmark is completed");
    }

    public String getDataHome() {
        String dir = home.getAbsolutePath();
        if (!(dir.endsWith("/") || dir.endsWith("\\"))) {
            dir += File.separator;
        }
        if (localData != null && localData.length() > 1 && localData.startsWith("/")) {
            localData = localData.substring(1, localData.length());
        }
        String datahome = dir + localData;
        return datahome;
    }

    public static void setup() {
        try {

            String schemaString = "message ycsb-usertable {" + "  required binary *rowkey (UTF8);"
                    + "  optional binary *hash (UTF8);" 
                    + "  optional binary *size;" 
                    + "  optional binary *type (UTF8);"
                    + "  optional binary *rowid (UTF8);" 
                    + "  optional binary *misc (UTF8);"
                    + "  optional binary *key (UTF8);" 
                    + "  optional binary *status (UTF8);"
                    + "  optional binary YCSB_KEY (UTF8);" 
                    + "  optional binary FIELD0 (UTF8);"
                    + "  optional binary FIELD1 (UTF8);" 
                    + "  optional binary FIELD2 (UTF8);"
                    + "  optional binary FIELD3 (UTF8);" 
                    + "  optional binary FIELD4 (UTF8);"
                    + "  optional binary FIELD5 (UTF8);" 
                    + "  optional binary FIELD6 (UTF8);"
                    + "  optional binary FIELD7 (UTF8);" 
                    + "  optional binary FIELD8 (UTF8);"
                    + "  optional binary FIELD9 (UTF8);" 
                    + " }";
            schema = MessageTypeParser.parseMessageType(schemaString);
        }
        catch (Exception e) {
            throw new Error("unable to initialize test database", e);
        }
    }

    long filename = 0;
    public final static String TABLE_FORMAT = "%08d";// 不用扩展名 "%s-%s-%08d%s";

    private String gendata(int maxSize, CompressionCodecName compressionType) throws Exception {
        filename++;
        String fileName = home.getAbsolutePath() + "/tmp/test" + maxSize + "_" + String.format(TABLE_FORMAT, filename)
                + TEST_EXT_NAME;
        ParquetDataWriter writer = null;
        try {
            writer = new ParquetDataWriter(fileName, schema, compressionType, ParquetFileWriter.Mode.CREATE);
            long i = 1;
            while (true) {
                Group group = new SimpleGroupFactory(schema).newGroup();
                group.append("*rowkey", Binary.fromConstantByteArray(Bytes.toBytes(i)));
                pushValueRandom(group, i);
                 
                writer.writeData(group);
                long datasize = writer.getDataSize();
                if (datasize >= (maxSize * SizeConstants.MB)) {
                    break;
                }
                i++;
            }
        }
        finally {
            if (writer != null) {
                try {
                    writer.close();
                }
                catch (IOException ioe) {
                    ioe.printStackTrace();
                }
            }
        }
        return fileName;
    }
 
    private Group pushValueRandom(Group group, long i) {
        String hex = Bytes.toHex(Bytes.toBytes(i));

        String hash = "ac8fb30d24c81754" + hex;
        String size = "00000489" + hex;
        String type = "143434343434343434343434" + hex;
        String rowid = "3230303031363933" + hex;
        String status = "00000000" + hex;

        group.append("*hash", Binary.fromConstantByteArray(Bytes.fromHex(hash)));
        group.append("*size", Binary.fromConstantByteArray(Bytes.fromHex(size)));
        group.append("*type", Binary.fromConstantByteArray(Bytes.fromHex(type)));
        group.append("*rowid", Binary.fromConstantByteArray(Bytes.fromHex(rowid)));
        group.append("*status", Binary.fromConstantByteArray(Bytes.fromHex(status)));

        group.append("YCSB_KEY", Binary.fromConstantByteArray(genBytes(12)));
        group.append("FIELD0", Binary.fromConstantByteArray(genBytes(100)));
        group.append("FIELD1", Binary.fromConstantByteArray(genBytes(100)));
        group.append("FIELD2", Binary.fromConstantByteArray(genBytes(100)));
        group.append("FIELD3", Binary.fromConstantByteArray(genBytes(100)));
        group.append("FIELD4", Binary.fromConstantByteArray(genBytes(100)));
        group.append("FIELD5", Binary.fromConstantByteArray(genBytes(100)));
        group.append("FIELD6", Binary.fromConstantByteArray(genBytes(100)));
        group.append("FIELD7", Binary.fromConstantByteArray(genBytes(100)));
        group.append("FIELD8", Binary.fromConstantByteArray(genBytes(100)));
        group.append("FIELD9", Binary.fromConstantByteArray(genBytes(100)));
        return group;
    }

    private byte[] genBytes(int len) {
        // String val = getStringRandom(len);
        String val = RandomStringUtils.randomAlphanumeric(len);
        byte[] bytes = val.getBytes();
        // System.out.println(val+"||"+bytes.length);
        return bytes;
    }
}
