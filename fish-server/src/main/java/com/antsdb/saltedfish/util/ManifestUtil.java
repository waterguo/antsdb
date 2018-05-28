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
import java.net.URL;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.jar.Manifest;

import org.apache.commons.lang.StringUtils;

/**
 * 
 * @author *-xguo0<@
 */
public final class ManifestUtil {
    public static Map<String, String> load(Class<?> klass) {
        Map<String, String> result = new HashMap<>();
        ClassLoader cl = klass.getClassLoader();
        String daklass = klass.getSimpleName() + ".class";
        URL daurl = klass.getResource(daklass);
        String prefix = StringUtils.substringBefore(daurl.toString(), "!");
        try {
            Enumeration<URL> it = cl.getResources("META-INF/MANIFEST.MF");
            while(it.hasMoreElements()) {
                URL url = it.nextElement();
                if (!url.toString().startsWith(prefix)) {
                    continue;
                }
                Manifest manifest = new Manifest(url.openStream());
                manifest.getMainAttributes().forEach((key, attr) -> {
                    result.put(key.toString(), attr.toString());
                });
                break;
            }
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
