/*-------------------------------------------------------------------------------------------------
 _______ __   _ _______ _______ ______  ______
 |_____| | \  |    |    |______ |     \ |_____]
 |     | |  \_|    |    ______| |_____/ |_____]

 Copyright (c) 2016, antsdb.com and/or its affiliates. All rights reserved. *-xguo0<@

 This program is free software: you can redistribute it and/or modify it under the terms of the
 GNU Affero General Public License, version 3, as published by the Free Software Foundation.

 You should have received a copy of the GNU Affero General Public License along with this program.
 If not, see <https://www.gnu.org/licenses/agpl-3.0.txt>
-------------------------------------------------------------------------------------------------*/
package com.antsdb.saltedfish.sql.mysql;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.RowIterator;
import com.antsdb.saltedfish.nosql.Statistician;
import com.antsdb.saltedfish.nosql.TableStats;
import com.antsdb.saltedfish.sql.vdm.CursorMeta;
import com.antsdb.saltedfish.sql.vdm.OpLike;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.SysTableRow;
import com.antsdb.saltedfish.sql.vdm.Transaction;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.sql.vdm.ViewMaker;
import com.antsdb.saltedfish.util.CursorUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class ShowTableStatus extends ViewMaker {
    private static final CursorMeta META = CursorUtil.toMeta(Line.class);
    
    private String db;
    @SuppressWarnings("unused")
    private String like;
    private Pattern pattern;

    public static class Line {
        public String Name;
        public String Engine;
        public Integer Version;
        public String Row_format;
        public Long Rows;
        public Long Avg_row_length;
        public Long Data_length;
        public Long Max_data_length;
        public Long Index_length;
        public Long Data_free;
        public Long Auto_increment;
        public Timestamp Create_time;
        public Timestamp Update_time;
        public Timestamp Check_time;
        public String Collation;
        public Long Checksum;
        public String Create_options;
        public String Comment;
    }
    
    public ShowTableStatus(String db, String like) {
        super(META);
        this.db = db;
        this.like = like;
        if (like != null) {
            this.pattern = OpLike.compile(like);
        }
    }

    @Override
    public Object run(VdmContext ctx, Parameters notused, long pMaster) {
        GTable systable = ctx.getMetaService().getSysTable();
        Transaction trx = ctx.getTransaction();
        RowIterator iter = systable.scan(trx.getTrxId(), trx.getTrxTs(), true);
        List<Line> result = new ArrayList<>();
        while (iter.next()) {
            SysTableRow row = new SysTableRow(iter.getRow());
            if (this.db != null) {
                if (!row.getNamespace().equalsIgnoreCase(this.db)) {
                    continue;
                }
            }
            if (this.pattern != null) {
                if (!pattern.matcher(row.getTableName()).matches()) {
                    continue;
                }
            }
            result.add(toLine(ctx, row));
        }
        return CursorUtil.toCursor(META, result);
    }

    private Line toLine(VdmContext ctx, SysTableRow table) {
        Line result = new Line();
        result.Name = table.getTableName();
        result.Engine = "InnoDB";
        result.Version = 10;
        result.Row_format = "Compact";
        result.Auto_increment = getAutoIncrement(ctx, table);
        result.Create_time = null;
        result.Update_time = null;
        result.Check_time = null;
        result.Collation = "utf8_general_ci";
        result.Checksum = null;
        result.Create_options = "";
        result.Comment = "";
        fillStats(ctx, table, result);
        return result;
    }

    private Long getAutoIncrement(VdmContext ctx, SysTableRow table) {
        return null;
    }

    private void fillStats(VdmContext ctx, SysTableRow table, Line result) {
        Statistician statistician = ctx.getHumpback().getStatistician();
        if (statistician == null) {
            return;
        }
        TableStats stats = statistician.getStats().get(table.getId());
        if (stats == null) {
            return;
        }
        result.Rows = stats.count;
        result.Avg_row_length = (long)stats.averageRowSize;
        result.Data_length = result.Rows * result.Avg_row_length;
        result.Max_data_length = null;
        result.Index_length = null;
        result.Data_free = null;
    }
}
