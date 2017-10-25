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

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.Map;
import java.util.jar.Manifest;

/**
 * 
 * @author *-xguo0<@
 */
public final class ManifestUtil {
    public static Map<String, String> load(Class<?> klass) {
        Map<String, String> result = new HashMap<>();
        URLClassLoader cl = (URLClassLoader)klass.getClassLoader();
        try {
            URL url = cl.findResource("META-INF/MANIFEST.MF");
            Manifest manifest = new Manifest(url.openStream());
            manifest.getMainAttributes().forEach((key, attr) -> {
                result.put(key.toString(), attr.toString());
            });
        } 
        catch (IOException x) {
        }
        return result;
    }
    
    public static void main(String[] args) {
        Map<String, String> map = load(ManifestUtil.class);
        System.out.println(map);
    }
}
