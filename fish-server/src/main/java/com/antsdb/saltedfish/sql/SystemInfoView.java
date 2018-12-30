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
package com.antsdb.saltedfish.sql;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import com.antsdb.saltedfish.cpp.MemoryManager;
import com.antsdb.saltedfish.sql.vdm.Cursor;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.sql.vdm.View;
import com.antsdb.saltedfish.util.CursorUtil;
import com.antsdb.saltedfish.util.LongLong;
import com.antsdb.saltedfish.util.MemoryUtil;
import com.antsdb.saltedfish.util.UberFormatter;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author wgu0
 */
public class SystemInfoView extends View {
    Orca orca;
    
    public SystemInfoView(Orca orca) {
        super(CursorUtil.toMeta(Properties.class));
        this.orca = orca;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        Map<String, Object> props = new HashMap<>();
        props.put("antsdb.last_sp", this.orca.getHumpback().getLatestSP());
        props.put("antsdb.memory_allocated", MemoryManager.getAllocated());
        props.put("antsdb.memory_pooled", MemoryManager.getPooled());
        props.put("antsdb.memory_immortal", getImmortalTotal());
        props.put("antsdb.trx_service_size", this.orca.getTrxMan().size());
        props.put("antsdb.trx_service_oldest", this.orca.getTrxMan().getOldest());
        props.put("antsdb.trx_service_last", this.orca.getTrxMan().getNewTrxId());
        props.put("antsdb.statistician_log_pointer", getStatisticianLogPointer());
        props.put("antsdb.storage_log_pointer", getStorageLogPointer());
        props.put("runtime.runtime_total_memory", Runtime.getRuntime().totalMemory());
        props.put("runtime.runtime_free_memory", Runtime.getRuntime().freeMemory());
        props.put("runtime.runtime_max_memory", Runtime.getRuntime().maxMemory());
        props.put("runtime.runtime_available_processors", Runtime.getRuntime().availableProcessors());
        props.put("vm.java_vm_info", System.getProperty("java.vm.info"));
        props.put("vm.java_vm_name", System.getProperty("java.vm.name"));
        props.put("vm.java_vm_vendor", System.getProperty("java.vm.vendor"));
        props.put("vm.java_vm_specification", System.getProperty("java.vm.specification.version"));
        props.put("vm.java_vm_version", System.getProperty("java.vm.version"));
        props.put("system.system cpu load", UberUtil.getSystemCpuLoad());
        props.put("system.process cpu load", UberUtil.getProcessCpuLoad());
        props.put("memory.direct_capacity", MemoryUtil.getDirectMemoryCapacity());
        props.put("memory.direct_used", MemoryUtil.getDirectMemoryUsed());
        props.put("memory.direct_max", MemoryUtil.getMaxDirectMemory());
        props.put("memory.jvm_heap_size", MemoryUtil.getHeapSize());
        props.put("memory.jvm_heap_used", MemoryUtil.getHeapUsage());
        for (GarbageCollectorMXBean gcbean:ManagementFactory.getGarbageCollectorMXBeans()) {
            props.put("gc." + gcbean.getName() + ".collection_time", gcbean.getCollectionTime());
            props.put("gc." + gcbean.getName() + ".collection_count", gcbean.getCollectionCount());
        }
        // done
        Cursor c = CursorUtil.toCursor(this.meta, props);
        return c;
    }

    private Long getImmortalTotal() {
        long total = 0;
        for (AtomicLong i:MemoryManager.getImmortals()) {
            total += i.get();
        }
        return total;
    }

    String getStatisticianLogPointer() {
        long result = orca.getHumpback().getStatistician().getReplicateLogPointer();
        return UberFormatter.hex(result);
    }
    
    String getStorageLogPointer() {
        LongLong result = orca.getHumpback().getStorageEngine().getLogSpan();
        return (result != null) ? UberFormatter.hex(result.y) : "";
    }
}
