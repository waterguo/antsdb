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

import java.util.HashSet;
import java.util.List;
import java.util.function.Consumer;

import com.antsdb.saltedfish.cpp.FishBool;
import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.Float8;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.meta.TableMeta;

/**
 * 
 * @author *-xguo0<@
 */
public class OpMatch extends Operator {
    
    private List<FieldValue> columns;
    private Operator against;
    private boolean isInWhere = true;
    private boolean isBooleanMode;

    public OpMatch(List<FieldValue> columns, Operator against, boolean isBooleanMode) {
        this.columns = columns;
        this.against = against;
        this.isBooleanMode = isBooleanMode;
    }
    
    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        return this.isInWhere ? evalInWhere(ctx, heap, params, pRecord) : evalInAggregator(ctx, heap, params, pRecord);
    }

    private long evalInAggregator(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        return Float8.allocSet(heap, 0.9999999f);
    }

    private long evalInWhere(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        long pQuery = this.against.eval(ctx, heap, params, pRecord);
        if (pQuery ==0) {
            return 0;
        }
        
        // tokenize query
        
        String query = (String)FishObject.get(heap, AutoCaster.toString(heap, pQuery));
        HashSet<String> termsToSearch = new HashSet<>();
        LuceneUtil.tokenize(query, (type, term) -> {
            termsToSearch.add(term.toLowerCase());
        });
        
        // tokenize content
        
        HashSet<String> terms = new HashSet<>();
        for (FieldValue i:columns) {
            long pText = i.eval(ctx, heap, params, pRecord);
            if (pText ==0) {
                continue;
            }
            String text = (String)FishObject.get(heap, AutoCaster.toString(heap, pText));
            LuceneUtil.tokenize(text, (type, term) -> {
                terms.add(term.toLowerCase());
            });
        }
        
        // match
        
        for (String term:termsToSearch) {
            if (terms.contains(term)) {
                return FishBool.allocSet(heap, true);
            }
        }
        return FishBool.allocSet(heap, false);
    }

    public TableMeta getTable() {
        return this.columns.get(0).getField().getTable();
    }
    
    public List<FieldValue> getColumns() {
        return columns;
    }

    public Operator getAgainst() {
        return against;
    }

    @Override
    public DataType getReturnType() {
        return this.isInWhere ? DataType.bool() : DataType.floatType();
    }

    @Override
    public void visit(Consumer<Operator> visitor) {
        visitor.accept(this);
        this.against.visit(visitor);
    }

    public void setWhere(boolean value) {
        this.isInWhere  = value;
    }

    public boolean isBooleanMode() {
        return this.isBooleanMode;
    }
}
