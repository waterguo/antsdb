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

import java.util.function.Consumer;
import java.util.regex.Pattern;

import com.antsdb.saltedfish.cpp.FishBool;
import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.sql.DataType;

/**
 * 
 * @author wgu0
 */
public class OpRegexp extends Operator {
	Operator expr;
	String text;
	Pattern pattern;
	
	public OpRegexp(Operator expr, String text) {
		this.expr = expr;
		this.text = text;
		StringBuilder buf = new StringBuilder();
		for (int i=0; i<text.length(); i++) {
			char ch = text.charAt(i);
			buf.append(ch);
		}
		this.pattern = Pattern.compile(buf.toString());
	}

	@Override
	public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
		long pValue = AutoCaster.toString(heap, this.expr.eval(ctx, heap, params, pRecord));
		if (pValue == 0) {
			return 0;
		}
		String s = (String)FishObject.get(heap, pValue);
		boolean result = this.pattern.matcher(s).find();
		return FishBool.allocSet(heap, result);
	}

	@Override
	public DataType getReturnType() {
		return DataType.bool();
	}

	@Override
	public void visit(Consumer<Operator> visitor) {
		visitor.accept(this);
		this.expr.visit(visitor);
	}
}
