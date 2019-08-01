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
package com.antsdb.saltedfish.util;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * 
 * @author *-xguo0<@
 */
public final class StreamUtil {
    public static <T> Stream<T> generate(Supplier<T> supplier) {
        Iterator<T> it = toIterator(supplier);
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(it, Spliterator.ORDERED), false);
    }
    
    public static <T> Iterator<T> toIterator(Supplier<T> supplier) {
        Iterator<T> result = new Iterator<T>() {
            T next;
            
            @Override
            public boolean hasNext() {
                return this.next != null;
            }

            @Override
            public T next() {
                T result = this.next;
                this.next = supplier.get();
                return result;
            }
        }; 
        result.next();
        return result;
    }
}
