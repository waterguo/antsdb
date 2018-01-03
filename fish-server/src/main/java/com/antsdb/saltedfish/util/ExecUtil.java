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
package com.antsdb.saltedfish.util;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;

/**
 * 
 * @author *-xguo0<@
 */
public class ExecUtil {
    private static class Monitor extends Thread {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        private InputStream in;

        Monitor(InputStream in) {
            this.in = in;
        }
        
        @Override
        public void run() {
            try {
                for (int value=this.in.read(); value>=0; value=this.in.read()) {
                    this.buf.write(value);
                }
            }
            catch (Exception x) {
            }
        }
    }
    
    public static ExecResult exec(String[] cmdarray) throws Exception {
        return exec(cmdarray, null);
    }
    
    public static ExecResult exec(String[] cmdarray, File dir) throws Exception {
        return exec(cmdarray, null, dir);
    }
    
    public static ExecResult exec(String[] cmdarray, String[] envp, File dir) throws Exception {
        Process process = Runtime.getRuntime().exec(cmdarray, envp, dir);
        Monitor stdoutMonitor = new Monitor(process.getInputStream());
        Monitor stderrMonitor = new Monitor(process.getErrorStream());
        stdoutMonitor.start();
        stderrMonitor.start();
        ExecResult result = new ExecResult();
        result.exit = process.waitFor();
        stdoutMonitor.join();
        stderrMonitor.join();
        result.stdout = stdoutMonitor.buf.toByteArray();
        result.stderr = stderrMonitor.buf.toByteArray();
        return result;
    }
}
