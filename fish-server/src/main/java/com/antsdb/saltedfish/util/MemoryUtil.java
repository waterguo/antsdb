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

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.List;

import com.sun.management.HotSpotDiagnosticMXBean;
import com.sun.management.VMOption;

/**
 * 
 * @author *-xguo0<@
 */
public final class MemoryUtil {
    public static long getHeapSize() {
        Runtime rt = Runtime.getRuntime();
        return rt.totalMemory();
    }
    
    public static long getHeapUsage() {
        Runtime rt = Runtime.getRuntime();
        return rt.totalMemory() - rt.freeMemory();
    }
    
    public static BufferPoolMXBean getDirectMemoryMXBean() {
        List<BufferPoolMXBean> pools = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
        for (BufferPoolMXBean pool:pools) {
            if (pool.getName().equals("direct")) {
                return pool;
            }
        }
        return null;
    }
    
    public static BufferPoolMXBean getMappedMemoryMXBean() {
        List<BufferPoolMXBean> pools = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
        for (BufferPoolMXBean pool:pools) {
            if (pool.getName().equals("mapped")) {
                return pool;
            }
        }
        return null;
    }
    
    public static long getDirectMemoryUsed() {
        return getDirectMemoryMXBean().getMemoryUsed();
    }
    
    public static long getDirectMemoryCapacity() {
        return getDirectMemoryMXBean().getTotalCapacity();
    }

    public static long getMaxDirectMemory() {
        long result = sun.misc.VM.maxDirectMemory();
        return result;
    }
    
    public static void main(String[] args) {
        List<BufferPoolMXBean> pools = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
        for (BufferPoolMXBean pool:pools) {
            System.out.println(String.format(
                    "%s %d/%d", 
                    pool.getName(), 
                    pool.getMemoryUsed(), 
                    pool.getTotalCapacity()));
        }
        RuntimeMXBean RuntimemxBean = ManagementFactory.getRuntimeMXBean();
        for (String arg:RuntimemxBean.getInputArguments()) {
            System.out.println(arg); 
        }
        HotSpotDiagnosticMXBean hsdiag = ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class);
        for (VMOption i:hsdiag.getDiagnosticOptions()) {
            System.out.println(i.getName() + ":" + i.getValue());
        }
        System.out.println(hsdiag.getVMOption("MaxDirectMemorySize"));
        System.out.println(sun.misc.VM.maxDirectMemory());
        System.out.println(getDirectMemoryUsed());
    }
}
