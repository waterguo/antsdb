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
package com.antsdb.saltedfish.nosql;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;

import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author wgu0
 */
public class HumpbackReadOnly {
    static Logger _log = UberUtil.getThisLogger();

    File home;
    File data;
	ConfigService config;
    GTableReadOnly sysmeta;
    Gobbler gobbler;
    SpaceManager spaceman;
    ConcurrentMap<Integer, GTableReadOnly> tableById = new ConcurrentHashMap<>();
    ConcurrentMap<String, String> namespaces = new ConcurrentHashMap<String, String>();

	public HumpbackReadOnly(File home) throws Exception {
    	this.home = home;
        this.data = new File(home, "data");
        this.config = new ConfigService(new File(this.home, "conf.properties"));
        _log.info("Humpback home: " + home.getAbsolutePath());
        init();
	}

    void init() throws Exception {
        if (!this.data.isDirectory()) {
        	throw new HumpbackException("home director is not found");
        }

        // init space
        
        this.spaceman = new SpaceManager(this.data, false, config.getSpaceFileSize());
        this.spaceman.init();
        
        // init logger
        
        this.gobbler = new Gobbler(this.spaceman, false);
        
    	// initialize sysmeta table
    	
    	initSysmeta();
    	
        // load data
        
        loadData();
    }

	private void initSysmeta() throws IOException {
        this.sysmeta = new GTableReadOnly(this, "", Integer.MAX_VALUE);
        this.sysmeta.open();
        if (this.sysmeta.getMemTable().getTabletsReadOnly().size() == 0) {
        	throw new HumpbackException("system metadata is not found");
        }
	}
	
	private void loadData() throws Exception {
        // load name spaces
        
        for (File i:this.data.listFiles()) {
        	if (i.isDirectory()) {
        		String ns = i.getName();
        		this.namespaces.put(ns.toLowerCase(), ns);
        	}
        }

    	// load tables
    	
        for (RowIterator i=this.sysmeta.scan(1, 1);;) {
        	if (!i.next()) {
        		break;
        	}
        	SysMetaRow row = new SysMetaRow(SlowRow.from(i.getRow()));
        	int id = row.getTableId();
        	String ns = row.getNamespace();
        	if (!this.namespaces.containsKey(ns.toLowerCase())) {
                String key = ns.toLowerCase();
                this.namespaces.putIfAbsent(key, ns);
        	}
            GTableReadOnly gtable = new GTableReadOnly(this, ns, id);
            gtable.open();
            this.tableById.put(gtable.getId(), gtable);
        }
	}

    public Collection<GTableReadOnly> getTables() {
        return this.tableById.values();
    }
    
    /**
     * namespace comparison is case insensitive
     * 
     * @param namespace
     * @return not nullable
     */
    public Collection<GTableReadOnly> getTables(String namespace) {
        List<GTableReadOnly> list = new ArrayList<>();
        for (GTableReadOnly i:this.tableById.values()) {
            if (i.getNamespace().equalsIgnoreCase(namespace)) {
                list.add(i);
            }
        }
        return list;
    }
    
    public GTableReadOnly getTable(int id) {
        if (id == Integer.MAX_VALUE) {
            return this.sysmeta;
        }
        GTableReadOnly table = this.tableById.get(id);
        return table;
    }

    public Gobbler getGobbler() {
    	return this.gobbler;
    }

	public SpaceManager getSpaceManager() {
		return this.spaceman;
	}

}
