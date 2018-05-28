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

import java.util.List;
import java.util.function.Consumer;

import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.sql.DataType;

public class NullValue extends Operator {

    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        return 0;
    }

    @Override
    public DataType getReturnType() {
        return null;
    }

    @Override
    public List<Operator> getChildren() {
        return null;
    }

    @Override
    public void visit(Consumer<Operator> visitor) {
        visitor.accept(this);
    }

	@Override
	public String toString() {
		return "NULL";
	}
}
