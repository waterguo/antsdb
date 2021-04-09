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
package com.antsdb.saltedfish.obs;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

public class LocalFileUtils {

    public static boolean renameHdfs(String srcFilename, String destFilename) throws IOException {
        File src = new File(srcFilename);
        File destFile = new File(destFilename);
        FileUtils.moveFile(src, destFile);
        return !src.exists() && destFile.exists();
    }

    public static boolean deleteDirLocal(String filePath) throws IOException {
        File directory = new File(filePath);
        FileUtils.deleteDirectory(directory);
        return !directory.exists();
    }

    public static long getFileSizeLocal(String fileAbsPathName) {
        return FileUtils.sizeOf(new File(fileAbsPathName));
    }

    public static boolean existsLocal(String fileAbsPathName) throws IOException {
        return new File(fileAbsPathName).exists();
    }

    public static boolean mkdirsLocal(String fileAbsPathName) throws IOException {
        return new File(fileAbsPathName).mkdirs();
    }

    public static boolean deleteFileLocal(String localAbsPath) throws IOException {
        File del = new File(localAbsPath);
        return del.delete();
    }
}
