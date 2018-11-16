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
package com.antsdb.saltedfish.sql.vdm;

import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.HumpbackSession;
import com.antsdb.saltedfish.nosql.TableType;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;

public class CreateColumn extends Statement implements ColumnAttributes {
    public ObjectName tableName;
    public String columnName;
    public DataType type;
    public boolean nullable;
    public String defaultValue;
    public boolean autoIncrement = false;
    public String enumValues;
    private String after;
    
    @Override
    public Object run(VdmContext ctx, Parameters params) {
        // create metadata
        
        Transaction trx = ctx.getTransaction();
        TableMeta table = Checks.tableExist(ctx.getSession(), this.tableName);
        ColumnMeta columnMeta = new ColumnMeta(
                ctx.getOrca(), 
                trx.getTrxId(),
                table,
                this.columnName
                );
        columnMeta.setType(this.type);
        columnMeta.setNullable(this.nullable);
        columnMeta.setDefault(this.defaultValue);
        columnMeta.setTimeId(ctx.getOrca().getIdentityService().getTimeId());
        columnMeta.setAutoIncrement(this.autoIncrement);
        columnMeta.setEnumValues(this.enumValues);

        // special table for blob data
        
        addBlobTableIfNecessary(ctx.getHumpback(), ctx.getHSession(), table, columnMeta);

        // after. if there is after, sequence number of the new column is between the after and after after column
        
        if (this.after != null) {
            ColumnMeta afterColumn = Checks.columnExist(table, this.after);
            int idxAfterAfter = table.getColumns().indexOf(afterColumn) + 1;
            float seqAfterAfter =  afterColumn.getSequence() + 0.5f;
            if (idxAfterAfter < table.getColumns().size()) {
                seqAfterAfter = table.getColumns().get(idxAfterAfter).getSequence();
            }
            columnMeta.setSequence((afterColumn.getSequence() + seqAfterAfter) / 2);
        }
        
        // done
        
        ctx.getOrca().getMetaService().addColumn(ctx.getHSession(), trx, table, columnMeta);
        
        return null;
    }

    @Override
    public String getColumnName() {
        return this.columnName;
    }

    @Override
    public ColumnAttributes setColumnName(String name) {
        this.columnName = name;
        return this;
    }

    @Override
    public DataType getType() {
        return this.type;
    }

    @Override
    public ColumnAttributes setType(DataType type) {
        this.type = type;
        return this;
    }

    @Override
    public boolean isNullable() {
        return this.nullable;
    }

    @Override
    public ColumnAttributes setNullable(boolean b) {
        this.nullable = b;
        return this;
    }

    @Override
    public String getDefaultValue() {
        return this.defaultValue;
    }

    @Override
    public ColumnAttributes setDefaultValue(String value) {
        this.defaultValue = value;
        return this;
    }

    @Override
    public boolean isAutoIncrement() {
        return this.isAutoIncrement();
    }

    @Override
    public ColumnAttributes setAutoIncrement(boolean value) {
        this.autoIncrement = value;
        return this;
    }

    public String getEnumValues() {
        return enumValues;
    }

    public void setEnumValues(String enumValues) {
        this.enumValues = enumValues;
    }

    public void setAfter(String after) {
        this.after = after;
    }

    static void addBlobTableIfNecessary(Humpback humpback, HumpbackSession hsession, TableMeta table, ColumnMeta column) {
        if (!column.isBlobClob()) {
            return;
        }
        int blobTableId = table.getBlobTableId();
        if (humpback.getTable(blobTableId) == null) {
            String name = String.format("%s_blob_", table.getTableName());
            humpback.createTable(hsession, table.getNamespace(), name, blobTableId, TableType.DATA);
        }
    }
    
}
