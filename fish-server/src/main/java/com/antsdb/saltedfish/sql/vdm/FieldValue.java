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

import java.util.Map;
import java.util.function.Consumer;

import com.antsdb.saltedfish.cpp.FishUtf8;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.Value;
import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.GetOptions;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.sql.planner.PlannerField;

/**
 * 
 * @author wgu0
 *
 */
public class FieldValue extends Operator {
    PlannerField field;
    byte fishType;
    boolean isEnum = false;
    
    public FieldValue(FieldMeta field, int pos) {
        this.field = new PlannerField(field, pos);
    }
    
    public FieldValue(PlannerField field) {
        this.field = field;
        this.fishType = field.getType().getFishType();
        if (field.getType().getName().equals("enum")) {
            this.isEnum = true;
        }
    }
    
    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        long pValue = Record.get(pRecord, this.field.getIndex());
        if ((pValue != 0) && (this.fishType == Value.TYPE_CLOB)) {
            return getClobValue(ctx, pValue);
        }
        if ((pValue != 0) && (this.fishType == Value.TYPE_BLOB)) {
            return getBlobValue(ctx, pValue);
        }
        if ((pValue != 0) && this.isEnum) {
            return getEnumValue(ctx, heap, pValue);
        }
        return pValue;
    }

    private long getEnumValue(VdmContext ctx, Heap heap, long pValue) {
        int value = AutoCaster.getInt(pValue);
        for (Map.Entry<String, Integer> i:field.getColumn().getEnumValueMap().entrySet()) {
            if (i.getValue() == value) {
                return FishUtf8.allocSet(heap, i.getKey());
            }
        }
        return 0;
    }

    private long getClobValue(VdmContext ctx, long pValue) {
        BlobReference ref = new BlobReference(pValue);
        TableMeta table = this.field.getTable();
        GTable blobTable = ctx.getHumpback().getTable(table.getBlobTableId());
        Transaction trx = ctx.getTransaction();
        long pKey = ref.getRowKeyAddress();
        long options = this.field.isNoCache() ? GetOptions.noCache(0) : 0;
        long pRow = blobTable.get(trx.getTrxId(), trx.getTrxTs(), pKey, options);
        if (pRow == 0) {
            return 0;
        }
        Row row = Row.fromMemoryPointer(pRow, Row.getVersion(pRow));
        pValue = row.getFieldAddress(field.getColumn().getColumnId());
        return pValue;
    }

    private long getBlobValue(VdmContext ctx, long pValue) {
        BlobReference ref = new BlobReference(pValue);
        TableMeta table = this.field.getTable();
        GTable blobTable = ctx.getHumpback().getTable(table.getBlobTableId());
        Transaction trx = ctx.getTransaction();
        long pKey = ref.getRowKeyAddress();
        long options = this.field.isNoCache() ? GetOptions.noCache(0) : 0;
        long pRow = blobTable.get(trx.getTrxId(), trx.getTrxTs(), pKey, options);
        if (pRow == 0) {
            return 0;
        }
        Row row = Row.fromMemoryPointer(pRow, Row.getVersion(pRow));
        pValue = row.getFieldAddress(field.getColumn().getColumnId());
        return pValue;
    }

    @Override
    public DataType getReturnType() {
        return this.field.getType();
    }

    @Override
    public void visit(Consumer<Operator> visitor) {
        visitor.accept(this);
    }

    @Override
    public String toString() {
        return this.field.toString();
    }

    public String getName() {
        return this.field.getName();
    }
    
    public PlannerField getField() {
        return this.field;
    }
}
