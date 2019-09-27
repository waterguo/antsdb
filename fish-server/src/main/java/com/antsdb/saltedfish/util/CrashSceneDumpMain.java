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
import java.sql.Timestamp;

import org.apache.commons.cli.Options;

import com.antsdb.saltedfish.cpp.BetterCommandLine;
import com.antsdb.saltedfish.util.AntiCrashCrimeScene.Unit;

/**
 * 
 * @author *-xguo0<@
 */
public class CrashSceneDumpMain extends BetterCommandLine {

    public static void main(String[] args) throws Exception {
        new CrashSceneDumpMain().parseAndRun(args);
    }

    @Override
    protected String getCommandName() {
        return "crash-scene-dump <file>";
    }

    @Override
    protected void buildOptions(Options options) {
        super.buildOptions(options);
    }

    @Override
    protected void run() throws Exception {
        if (this.cmdline.getArgs().length != 1) {
            println("error: file is missing");
            System.exit(-1);
        }
        File file = new File(this.cmdline.getArgs()[0]);
        if (!file.exists()) {
            println("error: file %s is not found", file);
            System.exit(-1);
        }
        AntiCrashCrimeScene scene = new AntiCrashCrimeScene(file);
        scene.open();
        for (Unit i:scene.getUnits()) {
            Timestamp ts = new Timestamp(i.getTimestamp());
            println("%d %s", i.getThreadId(), ts.toString());
        }
    }
}
