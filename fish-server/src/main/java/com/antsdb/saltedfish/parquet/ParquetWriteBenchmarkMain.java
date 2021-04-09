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
package com.antsdb.saltedfish.parquet;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import com.antsdb.saltedfish.cpp.BetterCommandLine;
import com.antsdb.saltedfish.nosql.ConfigService;
import com.antsdb.saltedfish.util.SizeConstants;

/**
 * 
 * @author lizc@tg-hd.com
 */
public class ParquetWriteBenchmarkMain extends BetterCommandLine {
    CommandLine line;
    private ConfigService config;
    private static MessageType schema;

    private final String TEST_EXT_NAME = ".test";

    public static void main(String[] args) throws Exception {
        new ParquetWriteBenchmarkMain().parseAndRun(args);
    }

    private File home;
    private String localDir;
    private boolean useRandom = true;
    private boolean useCreateMethod = true;

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
                    "Error:args param is not specified, Example ParquetBenchmarkMain home filesize(M) runtime(s) ");
            System.exit(-1);
        }
        List<String> params = line.getArgList();
        home = new File(params.get(0));
        int fileSize = Integer.parseInt(params.get(1));
        int runtime = Integer.parseInt(params.get(2)) * 1000;
        if (params.size() > 3) {
            useRandom = Boolean.parseBoolean(params.get(3));
        }
        File fileConfig = new File(home, "conf/conf.properties");
        if (!fileConfig.exists()) {
            println("error: config file is not found: %s", fileConfig);
            System.exit(-1);
        }
        setup();
        // connecting
        println("AntsDB home: %s,Write file size:%s(M),Plan run time:%s(ms) ", home, fileSize, runtime);
        File tmp = new File(home, "/tmp");
        if (tmp.exists() && tmp.isDirectory()) {
            File[] fi = tmp.listFiles((file, name) -> name.endsWith(TEST_EXT_NAME));
            for (File f : fi) {
                f.delete();
                println("clean tmp file : %s", f);
            }
        }
        this.config = new ConfigService(fileConfig);
        localDir = config.getLocalData();
        String compressCodec = config.getDatafileCompressionCodec();
        if (compressCodec == null || compressCodec.length() == 0) {
            compressCodec = "UNCOMPRESSED";
        }
        CompressionCodecName compressionType = CompressionCodecName.valueOf(compressCodec.toUpperCase());
        long minUseTime = Long.MAX_VALUE;
        long startRunTime = System.currentTimeMillis();
        long useTime = 0;
        while (true) {
            long startGenTime = System.currentTimeMillis();
            if(!useCreateMethod) {
                gendata(fileSize, compressionType);
            }
            else{
                gen(fileSize, compressionType);
            }
            long endGenTime = System.currentTimeMillis();
            long singleUseTime = endGenTime - startGenTime;
            if (singleUseTime < minUseTime) {
                minUseTime = singleUseTime;
            }
            println("Write (%s) %sM data use time:%s", compressionType, fileSize, singleUseTime);
            useTime = (endGenTime - startRunTime);
            if (useTime >= runtime) {
                break;
            }
        }
        long sumfileSize = 0;
        long fileCount = 0;
        if (tmp.exists() && tmp.isDirectory()) {
            File[] fi = tmp.listFiles((file, name) -> name.endsWith(TEST_EXT_NAME));
            fileCount = fi.length;
            for (File f : fi) {
                sumfileSize += f.length();
            }
        }
        if (tmp.exists() && tmp.isDirectory()) {
            File[] fi = tmp.listFiles((file, name) -> name.endsWith(TEST_EXT_NAME));
            for (File f : fi) {
                f.delete();
                println("clean tmp file : %s", f);
            }
        }
        BigDecimal sumdata = BigDecimal.valueOf(sumfileSize).divide(BigDecimal.valueOf(SizeConstants.MB), 3,
                RoundingMode.HALF_UP);
        BigDecimal perSecondUpload = BigDecimal.valueOf(sumfileSize).divide(BigDecimal.valueOf(useTime), 6,
                RoundingMode.HALF_UP);
        BigDecimal perSecondUploadS = perSecondUpload.multiply(BigDecimal.valueOf(1000));
        BigDecimal perSecondUploadMS = perSecondUploadS.divide(BigDecimal.valueOf(SizeConstants.MB), 3,
                RoundingMode.HALF_UP);

        BigDecimal second = BigDecimal.valueOf(useTime).divide(BigDecimal.valueOf(1000), 3, RoundingMode.HALF_UP);
        BigDecimal perSecondRecord = BigDecimal.valueOf(i).divide(second, 3, RoundingMode.HALF_UP);

        println("CompressCodec:%s,Single file size:%s(M),Sum file:(%s)%sbytes,%s(M),use time:%s(ms),"
                + "Throughput:%s(B/MS),%s(B/S),%s(M/S),Min time %s(MS),Sum Record:%s,Throughput:%s(Record/S)",
                compressCodec, fileSize, fileCount, sumfileSize, sumdata, useTime, perSecondUpload, perSecondUploadS,
                perSecondUploadMS, minUseTime, i, perSecondRecord);
        println("");
        println("done");
        println("parquet write benchmark is completed");
    }

    public String getDataHome() {
        String dir = home.getAbsolutePath();
        if (!(dir.endsWith("/") || dir.endsWith("\\"))) {
            dir += File.separator;
        }
        if (localDir != null && localDir.length() > 1 && localDir.startsWith("/")) {
            localDir = localDir.substring(1, localDir.length());
        }
        String datahome = dir + localDir;
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
    public final static String TABLE_FORMAT = "%08d";
    long i = 1;

    private String gendata(int maxSize, CompressionCodecName compressionType) throws Exception {
        filename++;
        String fileName = home.getAbsolutePath() + "/tmp/test" + maxSize + "_" + String.format(TABLE_FORMAT, filename)
                + TEST_EXT_NAME;
        ParquetDataWriter writer = null;
        try {
            writer = new ParquetDataWriter(fileName, schema, compressionType, ParquetFileWriter.Mode.CREATE);
            while (true) {
                Group group = new SimpleGroupFactory(schema).newGroup();
                group.append("*rowkey", Binary.fromConstantByteArray(Bytes.toBytes(i)));
                if (useRandom) {
                    pushValueRandom(group, i);
                }
                else {
                    pushValue(group, i);
                }
                if (i % 10000 == 0) {
                    println("Write (%s) ", i);
                }
                writer.writeData(group);
                long datasize =  writer.getDataSize();
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
    
    private static final int k = 1024; // 1 K byte
    private static final int m = 1024 * k;// 1 M byte

    private static final int _blockSize = 5 * m;

    private static final int _pageSize = 8 * k;
    private static final int _dictionaryPageSize = 5 * m;
    private static final boolean _enableDictionary = ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED;
    private static final boolean _validating = ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED;
    private static final WriterVersion _writerVersion = ParquetWriter.DEFAULT_WRITER_VERSION;
    
    private String gen(int maxSize,CompressionCodecName compressionType) throws IOException {
        filename++;
        String fileName = home.getAbsolutePath() + "/tmp/test" + maxSize + "_" + String.format(TABLE_FORMAT, filename)
                + TEST_EXT_NAME; 
        Path outFilePath = new Path(fileName);
        Configuration configuration = new Configuration();
        ParquetWriter<Group> fWriter = null;
        try {
            fWriter = ExampleParquetWriter.builder(outFilePath).withType(schema).withConf(configuration)
                    .withCompressionCodec(compressionType)
                    .withDictionaryEncoding(_enableDictionary)
                    .withWriterVersion(_writerVersion)
                    .withDictionaryPageSize(_dictionaryPageSize)
                    .withPageSize(_pageSize)
                    .withRowGroupSize(_blockSize)
                    .withValidation(_validating).build();
            long i = 1;
            while (true) {
                Group group = new SimpleGroupFactory(schema).newGroup();

                group.append("*rowkey", Binary.fromConstantByteArray(Bytes.toBytes(i)));
                if (useRandom) {
                    pushValueRandom(group, i);
                }
                else {
                    pushValue(group, i);
                }
                fWriter.write(group);
                if (i % 10000 == 0) {
                    println("Write (%s) ", i);
                }
                long datasize = fWriter.getDataSize();
                // _log.info("gen data :{},datasize:{}",i,datasize);
                if (datasize >= (maxSize * 1024 * 1024)) {
                    break;
                }
                i++;
            }

        }
        finally {
            if (fWriter != null) {
                try {
                    fWriter.close();
                }
                catch (IOException ioe) {
                    ioe.printStackTrace();
                }
            }
        }
        return fileName;
    }

    private Group pushValue(Group group, long i) {
        String hex = Bytes.toHex(Bytes.toBytes(i));

        String hash = "ac8fb30d24c81754" + hex;
        String size = "00000489" + hex;
        String type = "143434343434343434343434" + hex;
        String rowid = "3230303031363933" + hex;
        String status = "00000000" + hex;
        String ycsbkey = "757365723130303031363934" + hex;
        String field0 = "33507d3f507d25327c233a2e313062375d3d3d2432342e28365d61222c6e2433722e266c2020643749332d362825256e365d613a206e322c2e3c576d374839312c7c3e246423247c3649672f587b3e596f3330703242753f3b7c2e5163273a702d573935"
                + hex;
        String field1 = "20337c223030362f223d247430267c2f487532286a37257430252c295c373c4f3d2948613824682f24303a3b702a566d3e4e2f3d5a2b37362a284737355d6136512d31497b3a3f6626447126256e263136214a73255d2d31343c2b4a2d2d56612e243a34"
                + hex;
        String field2 = "3d5c6b2f4421265e2b2d5f79372a383d277c27313021422b3f3c743f4e73393e7e2f222c242c702b4f35343d2e2d213c3c4e3b3c2a303e4e633f3078204a213154793e5a2320327e2a417333356c3d522b202d362a4a3f215c652d407723476329447f2a"
                + hex;
        String field3 = "2458773b36343d45792246652321382d3772282e2c3c447b31342c343d743d2266262028283322242c602859733f2b32335239304f61215f39212578292c2c353c7e244133213732344a292d506d245177372c2a2b40393c2a70395a31293b2430236422"
                + hex;
        String field4 = "283c22394363263068232464374575262b743a2776362e22365e333c516528587d2a497d2c49632721662a28302c3170252a7a2f297c3e3072225877213f342443252643692a346a363c3e2935222b503739547129553521457925557f3338602c3d3e39"
                + hex;
        String field5 = "252428202b6e214b77232f323f503b215d63245b2f204a2d382838242a642e31702b4a733c2b26244d2937303630317c272538205b73202b2c3c41792727683b4f7f314d2328557f3f472b31206438207e3f577537546f223f623a4f2d284837234f2f34"
                + hex;
        String field6 = "3c547b325d6b22343a3425662f557726453b3e3938392a66283b62263228234b7f3f3828365b67334631362a302d436537587b225021334a6f3b2e383c25782f537b372d782e4b3b372934202c6e37442329477b3c487f32436d3729742b447f3a497d3e"
                + hex;
        String field7 = "3f47772a4061314b6f2f532b26253020542936342c282f20314161262a323d472d285231375f2b31306e2b5431343d3a2426722332262c246833536b265e39254a272c2d28385e6d304a3b382d78233a3431463f243d2c343538254c752251332b432738"
                + hex;
        String field8 = "2e302a2c5f752b42693641672f2d2c383e3e342a30343c20343a203034682f28202d5b793726762b2c282f497f3a413d29353a34342a3c3978334e772d5e6b2b2868245e7d322f7e2a5c633f32363e5e2120242e3555673333223526702f4b773525262d"
                + hex;
        String field9 = "25593b302662323266234d212850313528343b432730437936526b233e3039466536416b3544733a3238234465325f27393e20203a6e352e60322532324c3b39593d274c393d25762b5f3f212f2433393a252d662e2a683f502f35497923362e24562734"
                + hex;

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
        //String val = getStringRandom(len);
        String val = RandomStringUtils.randomAlphanumeric(len);
        byte[] bytes = val.getBytes();
        //System.out.println(val+"||"+bytes.length);
        return bytes;
    }

    public String Random(int RandomSize) {
        String serverRandom = "";
        Random rdm = new Random(System.currentTimeMillis());
        for (int i = 0; i < RandomSize / 2; i++) {
            String hex = Integer.toHexString(Math.abs(rdm.nextInt()) & 0xFF);
            if (hex.length() == 1) {
                hex = '0' + hex;
            }
            serverRandom += hex.toUpperCase();
        }
        return serverRandom;
    }

    public String getStringRandom(int length) {
        String val = "";
        Random random = new Random(length);
        // 参数length，表示生成几位随机数
        for (int i = 0; i < length; i++) {
            String charOrNum = random.nextInt(2) % 2 == 0 ? "char" : "num";
            // 输出字母还是数字
            if ("char".equalsIgnoreCase(charOrNum)) {
                // 输出是大写字母还是小写字母
                int temp = random.nextInt(2) % 2 == 0 ? 65 : 97;
                val += (char) (random.nextInt(26) + temp);
            }
            else if ("num".equalsIgnoreCase(charOrNum)) {
                val += String.valueOf(random.nextInt(10));
            }
        }
        return val;
    }
}
