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

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.Options;

import com.antsdb.saltedfish.cpp.BetterCommandLine;

/**
 * 
 * @author *-xguo0<@
 */
public class FindAddressMain extends BetterCommandLine {

    public static void main(String[] args) throws Exception {
        new FindAddressMain().parseAndRun(args);
    }

    @Override
    protected String getCommandName() {
        return "find-address <directory> <address>";
    }

    @Override
    protected void buildOptions(Options options) {
        super.buildOptions(options);
    }

    @Override
    protected void run() throws Exception {
        if (this.cmdline.getArgs().length != 2) {
            println("error: invalid arguments");
            return;
        }
        File home = new File(this.cmdline.getArgs()[0]);
        if (!home.isDirectory()) {
            println("error: directory %s is not found", home.toString());
            return;
        }
        String addrText = this.cmdline.getArgs()[1];
        long addr = 0;
        try {
            addr = parseHex(addrText);
        }
        catch (NumberFormatException x) {
            println("error: invalid hex address %s", addrText);
            return;
        }
        run(home, addr);
    }

    private void run(File home, long addr) throws Exception {
        for (File i:home.listFiles()) {
            if (i.isDirectory()) continue;
            find(i, addr);
        }
    }

    private void find(File file, long addr) throws Exception {
        Pattern ptn = Pattern.compile(".+nosql\\.MemoryMappedFile.+at(.+)with length(.+)");
        try (LineNumberReader reader = new LineNumberReader(new InputStreamReader(new FileInputStream(file)))) {
            for (;;) {
                String line = reader.readLine();
                if (line == null) break;
                Matcher m = ptn.matcher(line);
                if (m.find()) {
                    long start = parseHex(m.group(1));
                    long length = parseHex(m.group(2));
                    if (addr >= start && addr < (start+length)) {
                        println(line);
                        println("%x %x", start, length);
                    }
                }
            }
        }
    }

    private long parseHex(String value) {
        value = value.trim();
        if (!value.startsWith("0x")) {
            throw new NumberFormatException();
        }
        value = value.substring(2);
        return Long.parseLong(value, 16);
    }
}
