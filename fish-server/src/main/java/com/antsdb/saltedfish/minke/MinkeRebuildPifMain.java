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
package com.antsdb.saltedfish.minke;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.antsdb.saltedfish.cpp.BetterCommandLine;
import com.antsdb.saltedfish.cpp.SkipListScanner;
import com.antsdb.saltedfish.nosql.ConfigService;
import com.antsdb.saltedfish.nosql.TableType;

/**
 * 
 * @author *-xguo0<@
 */
public class MinkeRebuildPifMain extends BetterCommandLine {
    File home;
    File pif;
    long sp;
    ConfigService config;
    List<MinkeFile> mfiles = new ArrayList<>();
    
    @Override
    protected String getCommandName() {
        return "antsdb-rebuild-pif <directory> <sp> <pif file>";
    }

    public static void main(String[] args) throws Exception {
        new MinkeRebuildPifMain().parseAndRun(args);
    }
    
    @Override
    protected void run() throws Exception {
        if (this.cmdline.getArgs().length != 3) {
            println("error: invalid arguments");
            System.exit(-1);
        }
        this.home = new File(this.cmdline.getArgs()[0]);
        if (!this.home.isDirectory()) {
            println("error: home directory %s is not found", this.home.toString());
        }
        this.sp = Long.parseLong(this.cmdline.getArgs()[1]);
        this.pif = new File(this.cmdline.getArgs()[2]);
        rebuild();
    }

    private void rebuild() throws Exception {
        openConfig();
        openMinkeFiles();
        save();
    }

    private void save() throws IOException {
        Map<Integer, MinkeTable> tableById = new HashMap<>();
        for (MinkeFile i:this.mfiles) {
            for (MinkePage j:i.getPages()) {
                if (j.getSavedState() != PageState.CARBONFREEZED) {
                    continue;
                }
                MinkeTable mtable = tableById.get(j.getTableId());
                if (mtable == null) {
                    mtable = new MinkeTable(null, j.getTableId(), TableType.DATA);
                }
                findStartRange(j);
                println("found page: %s", j);
                mtable.pages.put(j);
            }
        }
        PageIndexFile pif = new PageIndexFile(this.pif);
        pif.save(tableById, this.sp);
        println("%s is rebuilt", this.pif);
    }

    private void findStartRange(MinkePage page) {
        SkipListScanner i = page.rows.scan(0, true, 0, true);
        if (!i.next()) {
            println("unable to find start range: %d", page.getId());
            System.exit(-1);
        }
        long pKey = i.getKeyPointer();
        println("%08x", pKey);
        page.pStartKey = pKey;
    }

    private void openMinkeFiles() throws IOException {
        int fileSize = config.getMinkeFileSize();
        int pageSize = config.getMinkePageSize();
        for (File i:this.home.listFiles()) {
            if (!i.getName().endsWith(".psf")) {
                continue;
            }
            int fileId = Integer.parseInt(i.getName().substring(0,  8), 16);
            println("found psf: %s", i);
            MinkeFile mfile = new MinkeFile(fileId, i, fileSize, pageSize, false);
            mfile.open();
            this.mfiles.add(mfile);
        }
        if (this.mfiles.size() == 0) {
            println("error: no psf files have been found");
            System.exit(-1);
        }
    }

    private void openConfig() throws Exception {
        File file = new File(this.home.getParentFile(), "conf/conf.properties");
        if (!file.exists()) {
            println("error: config file %s is not found", file);
            System.exit(-1);
        }
        this.config = new ConfigService(file);
    }

}
