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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;

import com.antsdb.saltedfish.util.UberUtil;

public class ParquetDataReader implements java.lang.AutoCloseable {
    static Logger _log = UberUtil.getThisLogger();

    private ParquetReader<Group> bufferedReader;

    private boolean hasNext = true;
    private String fileName;
    private MessageType schema;

    public boolean hashNext() {
        return hasNext;
    }

    public ParquetDataReader(Path inputPath, Configuration conf) {
        ParquetFileReader fileReader = null;
        try {
            bufferedReader = ParquetReader.builder(new GroupReadSupport(), inputPath)
                    .withConf(conf)
                    .build();
            
            ParquetReadOptions readOption = HadoopReadOptions
                            .builder(conf)
                            .build();
            
            InputFile file = HadoopInputFile.fromPath(inputPath, conf);
            fileReader = ParquetFileReader.open(file, readOption);
            if (_log.isTraceEnabled()) {
                PageReadStore page = fileReader.readNextRowGroup();
                if (page != null) {
                    _log.trace(" file={} RecordCount={} getRowCount={}", 
                            fileReader.getFile(),
                            fileReader.getRecordCount(), 
                            page.getRowCount());
                }
            }
            
            schema = fileReader.getFileMetaData().getSchema();
            closed = false;
        }
        catch (Exception e) {
            _log.error(fileName + "\t" + e.getMessage(), e);
            throw new OrcaObjectStoreException(e);
        }
        finally {
            if (fileReader != null) {
                try {
                    fileReader.close();
                }
                catch (IOException e) {
                    _log.error(fileName + "\t" + e.getMessage(), e);
                }
            }
        }
    }

    public MessageType getSchema() {
        return schema;
    }

    public Group readNext() throws IOException {
        return read();
    }

    public void closeReader() throws IOException {
        if (schema != null) {
            schema = null;
        }
        if (bufferedReader != null) {
            bufferedReader.close();
            bufferedReader = null;
        }
        closed = true;
    }

    @Override
    public void close() throws Exception {
        closeReader();
    }

    private boolean closed;

    public boolean isClosed() {
        return closed;
    }

    public Group read() throws IOException {
        if (bufferedReader == null || isClosed()) {
            return null;
        }
        Group g = bufferedReader.read();
        if (g != null) {
            hasNext = true;
        }
        return g;
    }
}
