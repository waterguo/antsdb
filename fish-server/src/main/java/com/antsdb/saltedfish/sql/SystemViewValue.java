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
package com.antsdb.saltedfish.sql;

import java.util.ArrayList;
import java.util.List;

import com.antsdb.saltedfish.cpp.MemoryManager;
import com.antsdb.saltedfish.sql.vdm.Cursor;
import com.antsdb.saltedfish.sql.vdm.CursorMaker;
import com.antsdb.saltedfish.sql.vdm.CursorMeta;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.util.CursorUtil;

/**
 * 
 * @author wgu0
 */
public class SystemViewValue extends CursorMaker {
	Orca orca;
	CursorMeta meta;
	
    public static class Item {
        public String NAME;
        public String VALUE;
    }

	public SystemViewValue(Orca orca) {
		this.orca = orca;
		meta = CursorUtil.toMeta(Item.class);
	}

	@Override
	public CursorMeta getCursorMeta() {
		return meta;
	}

	@Override
	public Object run(VdmContext ctx, Parameters params, long pMaster) {
        ArrayList<Item> list = new ArrayList<>();
        addItem(list, "antsdb.last_sp", this.orca.getHumpback().getLatestSP());
        addItem(list, "memory_allocated", MemoryManager.getAllocated());
        addItem(list, "memory_pooled", MemoryManager.getPooled());
        addItem(list, "runtime_total_memory", Runtime.getRuntime().totalMemory());
        addItem(list, "runtime_free_memory", Runtime.getRuntime().freeMemory());
        addItem(list, "runtime_max_memory", Runtime.getRuntime().maxMemory());
        addItem(list, "runtime_available_processors", Runtime.getRuntime().availableProcessors());
        addItem(list, "java_vm_info", System.getProperty("java.vm.info"));
        addItem(list, "java_vm_name", System.getProperty("java.vm.name"));
        addItem(list, "java_vm_vendor", System.getProperty("java.vm.vendor"));
        addItem(list, "java_vm_specification", System.getProperty("java.vm.specification.version"));
        addItem(list, "java_vm_version", System.getProperty("java.vm.version"));
        addItem(list, "trx_service_size", this.orca.getTrxMan().size());
        addItem(list, "trx_service_oldest", this.orca.getTrxMan().getOldest());
        addItem(list, "trx_service_last", this.orca.getTrxMan().getLastTrxId());
		
		// done
        Cursor c = CursorUtil.toCursor(meta, list);
        return c;
	}

	void addItem(List<Item> list, String name, String value) {
        Item item = new Item();
        list.add(item);
        item.NAME = name;
		item.VALUE = value;
	}

	void addItem(List<Item> list, String name, Integer value) {
		addItem(list, name, String.valueOf(value));
	}
	
	void addItem(List<Item> list, String name, Long value) {
		addItem(list, name, String.valueOf(value));
	}
}
