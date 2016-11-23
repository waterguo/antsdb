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

import java.io.IOException;
import java.util.function.BiConsumer;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;

/**
 * 
 * @author *-xguo0<@
 */
class LuceneUtil {
	static void tokenize(String text, BiConsumer<String, String> lambda) {
		try (StandardAnalyzer analyzer = new StandardAnalyzer()) {
			TokenStream stream = analyzer.tokenStream("", text);
			CharTermAttribute term = stream.getAttribute(CharTermAttribute.class);
			TypeAttribute type = stream.getAttribute(TypeAttribute.class);
			stream.reset();
			while (stream.incrementToken()) {
				lambda.accept(type.type(), term.toString());
			}
		}
		catch (IOException x) {
			throw new RuntimeException(x);
		}
	}
}
