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
package com.antsdb.saltedfish.nosql;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;

import com.antsdb.saltedfish.util.UberUtil;

/**
 * used to manage the backend jobs
 *  
 * @author *-xguo0<@
 */
public final class Scheduler {
    static final Logger _log = UberUtil.getThisLogger();
    
    private int ncpu = Runtime.getRuntime().availableProcessors();
    
    Map<String, Knob> knobs = new HashMap<>();
    AtomicInteger userLoad = new AtomicInteger();
    
    /**
     * 
     * @param name not null
     * @param priority 0-highest. 1-on when there are extra free system resource
     * @return
     */
    public Knob createKnob(String name, int priority) {
        Knob result = new Knob(this, name, priority);
        _log.debug("creatiing knob: {}", name);
        if (this.knobs.get(name) != null) throw new IllegalArgumentException(name);
        this.knobs.put(name, result);
        return result;
    }
    
    public Knob getKnob(String name) {
        return this.knobs.get(name);
    }
    
    public AtomicInteger getUserLoadCounter() {
        return this.userLoad;
    }

    /**
     * user payload in percentage. it can be more than 100.
     * 
     * @return
     */
    public int getUserLoad() {
        return this.userLoad.get() * 100 / this.ncpu;
    }

    public Collection<Knob> getKnobs() {
        return this.knobs.values();
    }
}
