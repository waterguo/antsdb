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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.hadoop.hbase.util.Bytes;

import com.antsdb.saltedfish.nosql.StorageEngine;
import com.antsdb.saltedfish.nosql.StorageTable;
import com.antsdb.saltedfish.parquet.ObsService;
import com.antsdb.saltedfish.parquet.bean.Partition;
import com.antsdb.saltedfish.sql.vdm.Cursor;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.sql.vdm.View;
import com.antsdb.saltedfish.util.CursorUtil;

/**
 * provides a list of partitions as a system view - antsdb.obs_partition
 * 
 * each row is a partition
 * each row has the following columns
 * 
 * TABLE_ID: table id
 * PARTITION_ID: partition id
 * NAME: name of the partition
 * LAST_ACCESS_TIME: last read/write time of the file
 * SIZE: number of bytes 
 * ROW_COUNT: number of rows
 * START_KEY: start key value
 * END_KEY: end key value
 * 
 * @author *-xguo0<@
 */
public class SystemViewObsPartitions extends View {
    public class Line {
        public Integer TABLE_ID;
        public String TABLE_TYPE;
        public Long PARTITION_ID;
        public String NAME;
        public Long SIZE;
        public Long ROW_COUNT;
        public Long CREATE_TIME;
        public Long LAST_UPLOAD_TIME;
        public Long LAST_ACCESS_TIME;
        public String START_KEY;
        public String END_KEY;
        public String DATA_PATH;
    }

    public SystemViewObsPartitions() {
        super(CursorUtil.toMeta(Line.class));
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        StorageEngine stor = ctx.getHumpback().getStorageEngine0();
        if (stor instanceof ObsService) {
            ObsService service = (ObsService) stor;
            Collection<StorageTable> tableInfos = service.getTableInfos();
            if (tableInfos == null) {
                return CursorUtil.toCursor(meta, Collections.emptyList());
            }
            ArrayList<Line> list = new ArrayList<>();
            for (StorageTable i : tableInfos) {
                ConcurrentSkipListMap<byte[], Partition> datas = i.getPartitions();
                if (datas == null || datas.size() == 0) {
                    continue;
                }
                String tableType = i.getRowMeta().getType().toString();
                for (Partition p : datas.values()) {
                    list.add(addLine(p,tableType));
                }
            }
            Cursor c = CursorUtil.toCursor(meta, list);
            return c;
        }
        else {
            return CursorUtil.toCursor(meta, Collections.emptyList());
        }
    }

    private Line addLine(Partition partition,String tableType) {
        Line result = new Line();
        result.TABLE_ID = partition.getTableId();
        result.TABLE_TYPE = tableType;
        result.PARTITION_ID = partition.getId();
        result.NAME = partition.getVersionFileName();
        result.SIZE = partition.getDataSize();
        result.ROW_COUNT = partition.getRowCount();
        result.CREATE_TIME = partition.getCreateTimestamp();
        result.LAST_UPLOAD_TIME = partition.getRemoteTimestamp();
        result.LAST_ACCESS_TIME = partition.getLastAccessTimestamp();
        result.START_KEY = Bytes.toHex(partition.getStartKey());
        result.END_KEY = Bytes.toHex(partition.getEndKey());
        result.DATA_PATH= partition.getDbName()+"/"+partition.getTableName()+"-"+partition.getTableId();
        return result;
    }
}
