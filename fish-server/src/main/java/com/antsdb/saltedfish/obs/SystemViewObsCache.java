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
import java.util.Collections;

import com.antsdb.saltedfish.nosql.StorageEngine;
import com.antsdb.saltedfish.obs.cache.ObsCache;
import com.antsdb.saltedfish.obs.cache.ObsFileReference;
import com.antsdb.saltedfish.parquet.ObsService;
import com.antsdb.saltedfish.sql.vdm.Cursor;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.sql.vdm.View;
import com.antsdb.saltedfish.util.CursorUtil;

/**
 * use SystemViewMinkePages as an example
 * 
 * provides file list  of the obs cache as a system view - antsdb.obs_cache
 * 
 * each row is a file in obs cache
 * each row has the following columns
 * 
 * FILE_NAME: name of the file
 * FILE_SIZE: size of the file
 * CREATE_TIME: time of the file creation
 * LAST_ACCESS_TIME: last read/write time of the file
 * FROM_OBS: if the file downloaded from obs or produced locally in the merge process
 * 
 * @author *-xguo0<@
 */
public class SystemViewObsCache extends View {
    public class Line {
        public String FILE_NAME;
        public Long FILE_SIZE;
        public Long CREATE_TIME;
        public Long LAST_ACCESS_TIME;
        public String FROM_OBS;
    }

    public SystemViewObsCache() {
        super(CursorUtil.toMeta(Line.class));
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        StorageEngine stor = ctx.getHumpback().getStorageEngine0();
        if (stor instanceof ObsService) {
            ObsService service = (ObsService) stor;
            ObsCache obsCache = service.getObsCache();
            if (obsCache == null) {
                return CursorUtil.toCursor(meta, Collections.emptyList());
            }
            ArrayList<Line> list = new ArrayList<>();
            for (ObsFileReference i : obsCache.getDatas()) {
                list.add(addLine(i));
            }
            Cursor c = CursorUtil.toCursor(meta, list);
            return c;
        }
        else {
            return CursorUtil.toCursor(meta, Collections.emptyList());
        }
    }

    private Line addLine(ObsFileReference ref) {
        Line result = new Line();
        result.FILE_NAME = ref.getKey();
        result.FILE_SIZE = ref.getFsize();
        result.CREATE_TIME = ref.getCreateTime();
        result.LAST_ACCESS_TIME = ref.getLastRead();
        result.FROM_OBS = ref.getSource();
        return result;
    }
}
