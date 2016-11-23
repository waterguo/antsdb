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

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.OrcaException;

public class FuncToTimestamp extends Function {
    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord)  {
        if (this.parameters.size() == 1) {
            long addrVal = this.parameters.get(0).eval(ctx, heap, params, pRecord);
            Object val = FishObject.get(heap, addrVal);
            return FishObject.allocSet(heap, Timestamp.valueOf(val.toString()));
        }
        else if (this.parameters.size() == 2) {
            long addrVal = this.parameters.get(0).eval(ctx, heap, params, pRecord);
            long addrFormat = this.parameters.get(1).eval(ctx, heap, params, pRecord);
            String format = (String)FishObject.get(heap, addrFormat);
            String val = (String)FishObject.get(heap, addrVal);
            if (!format.equals("YYYY-MM-DD HH24:MI:SS:FF6")) {
                throw new OrcaException("format is not supported: " + format);
            }
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
            val = val.substring(0, val.length()-3);
            try {
                return FishObject.allocSet(heap, new Timestamp(sdf.parse(val).getTime()));
            }
            catch (ParseException e) {
                throw new OrcaException("failed to parse timestamp: " + val);
            }
        }
        throw new OrcaException("wrong number of parameters");
    }

    @Override
    public DataType getReturnType() {
        return DataType.timestamp();
    }

    @Override
    public List<Operator> getChildren() {
        return null;
    }

	@Override
	public int getMinParameters() {
		return 1;
	}

}
