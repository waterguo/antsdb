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
package com.antsdb.saltedfish.nosql;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.antsdb.saltedfish.cpp.FileOffset;
import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.sql.FishCommandLine;
import com.antsdb.saltedfish.util.SizeConstants;
import com.antsdb.saltedfish.util.UberFormatter;

/**
 * 
 * @author *-xguo0<@
 */
public class HumpbackTraceIoMain extends FishCommandLine {

    public HumpbackTraceIoMain(String[] args) throws ParseException {
        super(args);
    }

    @Override
    protected Options getOptions() {
        Options options = new Options();
        return options;
    }

    @Override
    protected String getName() {
        return "fish-trace-io <table id> <rowkey>";
    }

    public static void main(String args[]) throws Exception {
        new HumpbackTraceIoMain(args).run();
    }

    private void run() throws Exception {
        if (this.cmd.getArgs().length != 2) {
            println("error: invalid arguments");
            System.exit(-1);
        }
        GTable gtable = findTable(this.cmd.getArgs()[0]);
        if (gtable == null) {
            println("error: table not found");
            System.exit(-1);
        }
        KeyBytes key = KeyBytes.fromHexDump(this.cmd.getArgs()[1]);
        trace(gtable, key);
    }

    private void trace(GTable gtable, KeyBytes key) {
        List<FileOffset> lines = new ArrayList<>();
        gtable.memtable.traceIo(key.getAddress(), lines);
        HashSet<Long> pages = new HashSet<>();
        for (FileOffset line:lines) {
            long pageId = getPageId(line);
            pages.add(pageId);
            println("%s:%s (%s)", line.file, UberFormatter.hex(line.offset), line.note);
        }
        println("total pages covered: %d", pages.size());
    }

    private long getPageId(FileOffset line) {
        long file = line.file.toString().hashCode();
        long page = line.offset / SizeConstants.kb(4);
        long result = (file << 32) | (page & 0xffffffff);
        return result;
    }
}
