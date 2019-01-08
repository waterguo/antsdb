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

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;

import org.apache.commons.lang.StringUtils;

public interface ConsoleHelper {
    public default void println(String format, Object... args) {
        String text = String.format(format, args);
        System.out.println(text);
    }

    public default void print(String format, Object... args) {
        String text = String.format(format, args);
        System.out.print(text);
    }
    
    public default void eprintln(String format, Object... args) {
        String text = String.format(format, args);
        System.err.println(text);
    }

    public default void eprint(String format, Object... args) {
        String text = String.format(format, args);
        System.err.print(text);
    }
    
    public default void printlnWithIndentation(String text, int spaces) {
        try {
            String indentation = StringUtils.repeat(" ", spaces);
            LineNumberReader reader = new LineNumberReader(new StringReader(text));
            for (String line=reader.readLine(); line != null; line=reader.readLine()) {
                println("%s%s", indentation, line);
            }
        }
        catch (IOException ignored) {}
    }
}
