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
package com.antsdb.saltedfish.sql.vdm;

import java.util.concurrent.ConcurrentHashMap;

import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.Int4;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.util.UberTime;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class FuncGetLock extends Function {
    static ConcurrentHashMap<String, LockEntry> _lockByName = new ConcurrentHashMap<>();
    
    static class LockEntry {
        volatile int sessionId;
        String name;
    }
    
    @Override
    public int getMinParameters() {
        return 2;
    }

    @Override
    public DataType getReturnType() {
        return DataType.integer();
    }

    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        long pLockName = this.parameters.get(0).eval(ctx, heap, params, pRecord);
        if (pLockName == 0) {
            return 0;
        }
        long pLockTimeout = this.parameters.get(1).eval(ctx, heap, params, pRecord);
        if (pLockTimeout == 0) {
            return 0;
        }
        String lockName = AutoCaster.getString(heap, pLockName);
        Integer lockTimeOut = AutoCaster.getInt(pLockTimeout);
        Integer result = lock(lockName, ctx.getSession().getId(), lockTimeOut);
        return result==null ? 0 : Int4.allocSet(heap, result);
    }

    static int lock(String lockName, int id, Integer lockTimeOut) {
        long start = UberTime.getTime();
        for (;;) {
            
            // timeout if waited too long
            
            if ((lockTimeOut >= 0) && (UberTime.getTime() - start > lockTimeOut * 1000)) {
                return 0;
            }
            
            // return 1 if we are able to successfully acquire the lock
            
            LockEntry lock = _lockByName.get(lockName);
            if (lock == null) {
                lock = new LockEntry();
                lock.name = lockName;
                lock.sessionId = id;
                if (_lockByName.putIfAbsent(lock.name, lock) != null) {
                    // sleep and wait if failed in racing
                    UberUtil.sleep(10);
                    continue;
                }
                return 1;
            }
            
            // return 1 if lock already acquired by this session
            
            if (lock.sessionId==id) {
                return 1;
            }
        
            // sleep and wait if lock is acquired by a different session

            UberUtil.sleep(10);
        }
    }

    static Integer unlock(String lockName, int id) {
        for (;;) {
            LockEntry lock = _lockByName.get(lockName);
            
            // return null if the lock is not found
            
            if (lock == null) {
                return null;
            }
            
            // return 0 if the lock is acquired by another session
            
            if (lock.sessionId != id) {
                return 0;
            }
            
            // sleep and wait if failed to remove the lock. must be some really wicked racing condition
            
            if (!_lockByName.remove(lock.name, lock)) {
                UberUtil.sleep(10);
                continue;
            }
            
            // success
            
            return 1;
        }
    }
}
