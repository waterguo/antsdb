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

import java.io.Closeable;
import java.util.Collections;
import java.util.Iterator;
import java.util.function.Function;
import java.util.function.Predicate;

public interface Flow<T> extends Closeable {
    
    static class IteratorFlow<T> implements Flow<T> {
        Iterator<T> iter;
        
        IteratorFlow(Iterator<T> iter) {
            this.iter = iter;
        }
        
        @Override
        public void close() {
        }

        @Override
        public T next() {
            return (iter.hasNext()) ? this.iter.next() : null;
        }

    }
    
    static class MappedFlow<T,R> implements Flow<R> {
        Flow<T> source;
        Function<? super T, ? extends R> mapper;
        
        MappedFlow(Flow<T> source, Function<? super T, ? extends R> mapper) {
            this.source = source;
            this.mapper = mapper;
        }
        
        @Override
        public void close() {
            this.source.close();
        }

        @Override
        public R next() {
            T value = this.source.next();
            return (value != null) ? this.mapper.apply(value) : null; 
        }

    }
    
    static class FilteredFlow<T> implements Flow<T> {
        Flow<T> source;
        Predicate<? super T> filter;
        
        FilteredFlow(Flow<T> source, Predicate<? super T> filter) {
            this.source = source;
            this.filter = filter;
        }
        
        @Override
        public void close() {
            this.source.close();
        }

        @Override
        public T next() {
            for (;;) {
                T value = this.source.next();
                if (value == null) {
                    return null;
                }
                if (this.filter.test(value)) {
                    return value;
                }
            }
        }
    }
    
    default public <R> Flow<R> map(Function<? super T, ? extends R> mapper) {
        return new MappedFlow<T,R>(this, mapper);
    }

    default public Flow<T> filter(Predicate<? super T> filter) {
        return new FilteredFlow<T>(this, filter);
    }
    
    public void close();
    
    public T next();
    
    public static <T> Flow<T> from(Iterable<T> iter) {
        return from(iter.iterator());
    }
    
    public static <T> Flow<T> from(Iterator<T> iter) {
        return new IteratorFlow<T>(iter);
    }
    
    public static <T> Flow<T> emptyFlow() {
        return from(Collections.emptyList());
    }
}
