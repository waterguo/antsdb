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

import java.time.Duration;
import java.time.LocalTime;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import com.antsdb.saltedfish.cpp.FishTime;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.sql.DataType;

public class CurrentTime extends Operator {

    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
    	LocalTime time = LocalTime.now();
    	Duration duration = Duration.ofSeconds(time.getHour() * 3600 + time.getMinute() * 60 + time.getSecond());
        long addr = FishTime.allocSet(heap, duration);
        return addr;
    }

    @Override
    public DataType getReturnType() {
        return DataType.time();
    }

    @Override
    public List<Operator> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public void visit(Consumer<Operator> visitor) {
        visitor.accept(this);
    }
}
