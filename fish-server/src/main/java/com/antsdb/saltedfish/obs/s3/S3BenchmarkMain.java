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
import com.antsdb.saltedfish.parquet.OrcaObjectStoreException;
import com.antsdb.saltedfish.parquet.ParquetDataWriter;
import com.antsdb.saltedfish.util.SizeConstants;

/**
 * 
 * @author lizc@tg-hd.com
 */
public class S3BenchmarkMain extends BetterCommandLine {
    CommandLine line;
    private ConfigService config;
    private static MessageType schema;

    private final String TEST_EXT_NAME = ".test";
    private final String S3TABLE_FORMAT = "benchmark_%s_%08d";
    private File home;
    String localData;

    public static void main(String[] args) throws Exception {
        new S3BenchmarkMain().parseAndRun(args);
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

        S3Config s3config = new S3Config(config.getS3ServiceEndpoint(), config.getS3Region(), config.getS3accessKey(),
                config.getS3secretKey(), config.getS3BucketName());

        if (s3config.getClientRegion() == null || s3config.getClientRegion().length() == 0
                || s3config.getAccessKey() == null || s3config.getSecretKey() == null) {
            println("s3 auth info cannot be empty or capitalized");
            throw new OrcaObjectStoreException("s3 auther info cannot be empty or capitalized");
        }
        ObsProvider client = new S3StoreProvider(s3config);
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
                    + "  optional binary *hash (UTF8);" + "  optional binary *size;" + "  optional binary *type (UTF8);"
                    + "  optional binary *rowid (UTF8);" + "  optional binary *misc (UTF8);"
                    + "  optional binary *key (UTF8);" + "  optional binary *status (UTF8);"
                    + "  optional binary YCSB_KEY (UTF8);" + "  optional binary FIELD0 (UTF8);"
                    + "  optional binary FIELD1 (UTF8);" + "  optional binary FIELD2 (UTF8);"
                    + "  optional binary FIELD3 (UTF8);" + "  optional binary FIELD4 (UTF8);"
                    + "  optional binary FIELD5 (UTF8);" + "  optional binary FIELD6 (UTF8);"
                    + "  optional binary FIELD7 (UTF8);" + "  optional binary FIELD8 (UTF8);"
                    + "  optional binary FIELD9 (UTF8);" + " }";
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
                pushValue(group);
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

    private Group pushValue(Group group) {
        String hash = "ac8fb30d24c81754";
        String size = "00000489";
        String type = "143434343434343434343434";
        String rowid = "3230303031363933";
        String status = "00000000";
        String ycsbkey = "757365723130303031363934";
        String field0 = "33507d3f507d25327c233a2e313062375d3d3d2432342e28365d61222c6e2433722e266c2020643749332d362825256e365d613a206e322c2e3c576d374839312c7c3e246423247c3649672f587b3e596f3330703242753f3b7c2e5163273a702d573935";
        String field1 = "20337c223030362f223d247430267c2f487532286a37257430252c295c373c4f3d2948613824682f24303a3b702a566d3e4e2f3d5a2b37362a284737355d6136512d31497b3a3f6626447126256e263136214a73255d2d31343c2b4a2d2d56612e243a34";
        String field2 = "3d5c6b2f4421265e2b2d5f79372a383d277c27313021422b3f3c743f4e73393e7e2f222c242c702b4f35343d2e2d213c3c4e3b3c2a303e4e633f3078204a213154793e5a2320327e2a417333356c3d522b202d362a4a3f215c652d407723476329447f2a";
        String field3 = "2458773b36343d45792246652321382d3772282e2c3c447b31342c343d743d2266262028283322242c602859733f2b32335239304f61215f39212578292c2c353c7e244133213732344a292d506d245177372c2a2b40393c2a70395a31293b2430236422";
        String field4 = "283c22394363263068232464374575262b743a2776362e22365e333c516528587d2a497d2c49632721662a28302c3170252a7a2f297c3e3072225877213f342443252643692a346a363c3e2935222b503739547129553521457925557f3338602c3d3e39";
        String field5 = "252428202b6e214b77232f323f503b215d63245b2f204a2d382838242a642e31702b4a733c2b26244d2937303630317c272538205b73202b2c3c41792727683b4f7f314d2328557f3f472b31206438207e3f577537546f223f623a4f2d284837234f2f34";
        String field6 = "3c547b325d6b22343a3425662f557726453b3e3938392a66283b62263228234b7f3f3828365b67334631362a302d436537587b225021334a6f3b2e383c25782f537b372d782e4b3b372934202c6e37442329477b3c487f32436d3729742b447f3a497d3e";
        String field7 = "3f47772a4061314b6f2f532b26253020542936342c282f20314161262a323d472d285231375f2b31306e2b5431343d3a2426722332262c246833536b265e39254a272c2d28385e6d304a3b382d78233a3431463f243d2c343538254c752251332b432738";
        String field8 = "2e302a2c5f752b42693641672f2d2c383e3e342a30343c20343a203034682f28202d5b793726762b2c282f497f3a413d29353a34342a3c3978334e772d5e6b2b2868245e7d322f7e2a5c633f32363e5e2120242e3555673333223526702f4b773525262d";
        String field9 = "25593b302662323266234d212850313528343b432730437936526b233e3039466536416b3544733a3238234465325f27393e20203a6e352e60322532324c3b39593d274c393d25762b5f3f212f2433393a252d662e2a683f502f35497923362e24562734";

        group.append("*hash", Binary.fromConstantByteArray(Bytes.fromHex(hash)));
        group.append("*size", Binary.fromConstantByteArray(Bytes.fromHex(size)));
        group.append("*type", Binary.fromConstantByteArray(Bytes.fromHex(type)));
        group.append("*rowid", Binary.fromConstantByteArray(Bytes.fromHex(rowid)));
        group.append("*status", Binary.fromConstantByteArray(Bytes.fromHex(status)));
        group.append("YCSB_KEY", Binary.fromConstantByteArray(Bytes.fromHex(ycsbkey)));
        group.append("FIELD0", Binary.fromConstantByteArray(Bytes.fromHex(field0)));
        group.append("FIELD1", Binary.fromConstantByteArray(Bytes.fromHex(field1)));
        group.append("FIELD2", Binary.fromConstantByteArray(Bytes.fromHex(field2)));
        group.append("FIELD3", Binary.fromConstantByteArray(Bytes.fromHex(field3)));
        group.append("FIELD4", Binary.fromConstantByteArray(Bytes.fromHex(field4)));
        group.append("FIELD5", Binary.fromConstantByteArray(Bytes.fromHex(field5)));
        group.append("FIELD6", Binary.fromConstantByteArray(Bytes.fromHex(field6)));
        group.append("FIELD7", Binary.fromConstantByteArray(Bytes.fromHex(field7)));
        group.append("FIELD8", Binary.fromConstantByteArray(Bytes.fromHex(field8)));
        group.append("FIELD9", Binary.fromConstantByteArray(Bytes.fromHex(field9)));

        return group;

    }

}
