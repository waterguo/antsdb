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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;

import com.antsdb.saltedfish.util.UberUtil;

public class ParquetDataWriter implements java.lang.AutoCloseable {
    Logger _log = UberUtil.getThisLogger();

    private ParquetWriter<Group> fWriter = null;

    public static final int minParquetSize = 5;//min parquet file size byte

    private static int _blockSize =  ParquetWriter.DEFAULT_BLOCK_SIZE;//128 * 1024 * 1024    512 * m;128 * SizeConstants.KB; ;

    private static final int _pageSize = ParquetWriter.DEFAULT_PAGE_SIZE;//1024 * 1024   8 * k;
    private static final int _dictionaryPageSize = ParquetProperties.DEFAULT_DICTIONARY_PAGE_SIZE;//1024 * 1024   5 * m;
    private static final boolean _enableDictionary = ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED;
    private static final boolean _validating = ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED;

    private static final WriterVersion _writerVersion = ParquetWriter.DEFAULT_WRITER_VERSION;//  WriterVersion.PARQUET_2_0;antsdb会多记录
    private CompressionCodecName _compressionCodecName = CompressionCodecName.SNAPPY;

    private boolean close;
    private MessageType schema;
    
    private long rowCount;
    private long dataSize;
    
    public ParquetDataWriter(String fileName, MessageType schema, CompressionCodecName compressionCodecName,
            ParquetFileWriter.Mode mode) {
        _compressionCodecName = compressionCodecName;
        this.schema = schema;
        Path outFilePath = new Path(fileName);
        Configuration configuration = new Configuration();
        //configuration.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
        try { 
            configuration.set("dfs.blocksize", String.valueOf(_blockSize)); 
            fWriter = ExampleParquetWriter.builder(outFilePath)
                    .withType(schema)
                    .withConf(configuration)
                    .withCompressionCodec(_compressionCodecName)
                    .withDictionaryEncoding(_enableDictionary)
                    .withWriteMode(mode)
                    .withWriterVersion(_writerVersion)
                    .withDictionaryPageSize(_dictionaryPageSize)
                    .withPageSize(_pageSize)
                    .withRowGroupSize(_blockSize)
                    .withValidation(_validating)
                    .build();
            close = false;
            this.rowCount = 0;
        }
        catch (Exception e) {
            throw new OrcaObjectStoreException(e,"create parquet file={} error.",fileName);
        }
    }

    public void writeData(Group data) {
        try {
            fWriter.write(data);
            this.dataSize = fWriter.getDataSize();
            this.rowCount = this.rowCount + 1; 
        }
        catch (Exception e) {
            _log.error("error: {},data{},exception:{}", schema,data, e); 
            throw new OrcaObjectStoreException(e);
        }
    }
    
    public long getDataSize() {
        return this.dataSize;
    }

    public void closeFileWriter() {
        if (fWriter != null) {
            try {
                fWriter.close();
            }
            catch (IOException e) {
                _log.error(e.getMessage(), e);
            }
        }
        close = true;
        fWriter = null;
    }

    public boolean isClose() {
        return close;
    }

    @Override
    public void close() throws Exception {
        closeFileWriter();
    }

    public void writeDatas(List<Group> datas) throws IOException {
        if (datas != null && datas.size() > 0) {
            for (Group data : datas) {
                fWriter.write(data);
                rowCount ++;
            }

        }
    }

    public long getRowCount() {
        return this.rowCount;
    }
    
    
}
