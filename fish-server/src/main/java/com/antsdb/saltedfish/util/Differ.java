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

import java.util.Comparator;
import java.util.Iterator;

/**
 * 
 * @author *-xguo0<@
 */
public class Differ<T> {
    private Iterator<T> streamx;
    private Iterator<T> streamy;
    private Comparator<T> comparator;

    public static enum State {
        ADD,
        DELETE,
        CHANGE,
        EQUAL,
    }
    
    static class Cursor<T> {
        Iterator<T> it;
        T current;
        
        Cursor(Iterator<T> it) {
            this.it = it;
        }
        
        boolean next() {
            if (!this.it.hasNext()) {
                this.current = null;
                return false;
            }
            this.current = it.next();
            return true;
        }
        
        T value() {
            return this.current;
        }
    }
    
    public Differ(Iterator<T> x, Iterator<T> y, Comparator<T> comp) {
        this.streamx = x;
        this.streamy = y;
        this.comparator = comp;
    }
    
    public void diff(TriConsumer<State, T, T> callback) {
        Cursor<T> cx = new Cursor<>(streamx);
        Cursor<T> cy = new Cursor<>(streamy);
        cx.next();
        cy.next();
        while ((cx.value() != null) || (cy.value() != null)) {
            T x = cx.value();
            T y = cy.value();
            if (x == null) {
                callback.accept(State.ADD, null, y);
                cy.next();
                continue;
            }
            if (y == null) {
                callback.accept(State.DELETE, x, null);
                cx.next();
                continue;
            }
            int comp = this.comparator.compare(x, y);
            if (comp < 0) {
                callback.accept(State.DELETE, x, null);
                cx.next();
                continue;
            }
            else if (comp > 0) {
                callback.accept(State.ADD, null, y);
                cy.next();
                continue;
            }
            else {
                callback.accept(State.EQUAL, x, y);
                cx.next();
                cy.next();
                continue;
            }
        }
    }
}
