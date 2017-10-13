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
package com.antsdb.saltedfish.minke;

import java.io.File;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.varia.NullAppender;

import com.antsdb.saltedfish.cpp.BetterCommandLine;
import com.antsdb.saltedfish.nosql.ConfigService;

/**
 * 
 * @author *-xguo0<@
 */
public abstract class MinkeCommandLine extends BetterCommandLine {
    static {
        String level = System.getenv("ANTSDB_LOG_LEVEL");
        BasicConfigurator.resetConfiguration();
        if (level != null) {
            BasicConfigurator.resetConfiguration();
            BasicConfigurator.configure();
            Logger.getRootLogger().setLevel(Level.toLevel(level));
        }
        else {
            BasicConfigurator.resetConfiguration();
            BasicConfigurator.configure(new NullAppender());
        }
    }
    
    public File getHome() {
        String home = this.cmdline.getOptionValue("home");
        if (home == null) {
            String fishhome = System.getenv("ANTSDB_HOME");
            if (new File(fishhome, "cache").exists()) {
                home = new File(fishhome, "cache").getPath();
            }
            else {
                home = new File(fishhome, "data").getPath();
            }
        }
        if (home == null) {
            println("error: home directory is not specified");
            return null;
        }
        File file = new File(home);
        if (!new File(file, "00000100.psf").exists()) {
            println("error: home directory '%s' is invalid", file.getAbsolutePath());
            return null;
        }
        return file;
    }
    
    public Minke getMinke() throws Exception  {
        File home = getHome();
        if (home == null) { 
            return null;
        }
        Minke minke = new Minke();
        minke.open(getHome(), new ConfigService(), false);
        return minke;
    }
}
