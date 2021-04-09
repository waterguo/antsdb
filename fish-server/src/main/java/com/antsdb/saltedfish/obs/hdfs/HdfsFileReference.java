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
package com.antsdb.saltedfish.obs.hdfs;

import java.io.File;

 
public class HdfsFileReference {
    private File file;
    
    public HdfsFileReference( File file) {
        this.file = file; 
    }

     

    @Override
    public void finalize() {
        this.file.delete();
    }



    public long length() {
       if(this.file != null) {
           return this.file.length();
       }
        return 0;
    }



    public Object getAbsolutePath() {
        if(this.file != null) {
            return this.file.getAbsolutePath();
        }
        return null;
    }
}
