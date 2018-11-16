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

import com.antsdb.saltedfish.cpp.FishNumber;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.Int8;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.IdentifierService;
import com.antsdb.saltedfish.sql.meta.SequenceMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;

/**
 * mysql autoincrement
 * 
 * @author wgu0
 */
public class OpIncrementColumnValue extends UnaryOperator {

    private TableMeta table;
    
    public OpIncrementColumnValue(TableMeta table, Operator upstream) {
        super(upstream);
        this.table = table;
    }

    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        if (upstream != null) {
            long pValue = this.upstream.eval(ctx, heap, params, pRecord);
            if (pValue != 0) {
                long pNumber = AutoCaster.toNumber(heap, pValue);
                long val = FishNumber.longValue(pNumber);
                if (val != 0 || ctx.getSession().getConfig().isNoAutoValueOnZero()) {
                    // if user set a value to an auto_increment column, we need to remember it
                    SequenceMeta seq = getSequence(ctx);
                    if (val > seq.getLastNumber()) {
                        seq.setLastNumber(val);
                        ctx.getMetaService().updateSequence(ctx.getHSession(), seq.getTransactionTimestamp(), seq);
                    }
                    return pNumber;
                }
            }
        }
        IdentifierService idService = ctx.getOrca().getIdentityService();
        long value = idService.getNextId(this.table.getAutoIncrementSequenceName());
        ctx.getSession().setLastInsertId(value);
        long addr = Int8.allocSet(heap, value);
        return addr;
    }

    SequenceMeta getSequence(VdmContext ctx) {
        Transaction trx = ctx.getTransaction();
        ObjectName name = this.table.getAutoIncrementSequenceName();
        return ctx.getMetaService().getSequence(trx, name);
    }
    
    @Override
    public DataType getReturnType() {
        return DataType.longtype();
    }
    
}
