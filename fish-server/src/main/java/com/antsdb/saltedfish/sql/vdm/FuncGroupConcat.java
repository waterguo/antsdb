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
package com.antsdb.saltedfish.sql.vdm;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;

import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.sql.DataType;

/**
 * 
 * @author wgu0
 */
public class FuncGroupConcat extends Function {
    int variableId;
    Boolean asc;
    boolean distinct;

    private static class Data {
        String seprator = ",";
        Collection<String> words;
    }

    public FuncGroupConcat(int variableId, boolean distinct, Boolean asc) {
        this.variableId = variableId;
        this.distinct = distinct;
        this.asc = asc;
    }

    private Data createData(String separator) {
        Data result = new Data();
        result.seprator = separator;
        if (this.distinct) {
            if (this.asc == null) {
                result.words = new ArrayList<>();
            }
            else {
                result.words = new TreeSet<>(createComparator());
            }
        }
        else {
            if (this.asc == null) {
                result.words = new ArrayList<>();
            }
            else {
                result.words = new PriorityQueue<>(createComparator());
            }
        }
        return result;
    }
    
    private Comparator<String> createComparator() {
        boolean asc = this.asc;
        Comparator<String> result = new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return asc ? o1.compareTo(o2) : o2.compareTo(o1);
            }
        };
        return result;
    }

    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        // initialize

        Data data = (Data) ctx.getVariable(this.variableId);
        if (data == null) {
            String separator = ",";
            if (this.parameters.size() >= 2) {
                long pSeparator = this.parameters.get(1).eval(ctx, heap, params, pRecord);
                separator = AutoCaster.getString(heap, pSeparator);
            }
            data = createData(separator);
            ctx.setVariable(variableId, data);
        }
        if (Record.isGroupEnd(pRecord)) {
            data.words.clear();
            return 0;
        }
        long addrVal = this.parameters.get(0).eval(ctx, heap, params, pRecord);
        addrVal = AutoCaster.toString(heap, addrVal);
        String val = (String) FishObject.get(heap, addrVal);
        if (val != null) {
            data.words.add(val);
        }
        return FishObject.allocSet(heap, StringUtils.join(data.words, data.seprator));
    }

    @Override
    public DataType getReturnType() {
        return DataType.varchar();
    }

    @Override
    public int getMinParameters() {
        return 1;
    }

    @Override
    public int getMaxParameters() {
        return 2;
    }
}
