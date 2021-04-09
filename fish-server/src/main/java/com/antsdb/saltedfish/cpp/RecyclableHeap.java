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
package com.antsdb.saltedfish.cpp;

import org.slf4j.Logger;

import com.antsdb.saltedfish.util.UberUtil;

/**
 * WARNING, not thread safe
 * 
 * @author *-xguo0<@
 */
public final class RecyclableHeap extends Heap {
    @SuppressWarnings("unused")
    private static Logger _log = UberUtil.getThisLogger(); 
    private static final int OFFSET_NEXT = 0;
    private static final int OFFSET_SIZE = 8;
    private static final int OFFSET_END = 0xc;
    
    private Heap parent;
    private long[] blocks = new long[32];
    private boolean freeParent;
    private int unitSizeEstimate = 128;
    private long pUnit;
    private long parentPos;
    private long capacity;
    private long usage;
    private long free;

    private final static class Unit {
        private static final int OFFSET_HEAD = 0;
        private static final int OFFSET_CURRENT = 0x8;
        private static final int OFFSET_FREE = 0x10;
        private static final int OFFSET_LIMIT = 0x18;
        private static final int OFFSET_END = 0x20;
        
        private long addr;

        Unit(long addr) {
            this.addr = addr;
        }
        
        public long getLimit() {
            return Unsafe.getLong(this.addr + OFFSET_LIMIT);
        }
        
        public void setLimit(long value) {
            Unsafe.putLong(this.addr + OFFSET_LIMIT, value);
        }
        
        public long getFree() {
            return Unsafe.getLong(this.addr + OFFSET_FREE);
        }

        public void setFree(long value) {
            Unsafe.putLong(this.addr + OFFSET_FREE, value);
        }

        public long getCurrentBlock() {
            return Unsafe.getLong(this.addr + OFFSET_CURRENT);
        }

        public void setCurrentBlock(long value) {
            Unsafe.putLong(this.addr + OFFSET_CURRENT, value);
        }

        public long getHeadBlock() {
            return Unsafe.getLong(this.addr + OFFSET_HEAD);
        }

        public void setHeadBlock(long value) {
            Unsafe.putLong(this.addr + OFFSET_HEAD, value);
        }

        public long getStart() {
            return this.addr + OFFSET_END;
        }
    }
    
    public RecyclableHeap(Heap parent, boolean freeParent) {
        this.parent = parent;
        this.freeParent = freeParent;
        this.parentPos = parent.position();
    }
    
    @Override
    public long alloc(int size, boolean zero) {
        if (this.pUnit == 0) {
            allocUnit(size);
        }
        return alloc(this.pUnit, size, zero);
    }
    
    public long alloc(long pUnit2Use, int size, boolean zero) {
        Unit unit = new Unit(pUnit2Use);
        long pFree;
        for (;;) {
            pFree = unit.getFree();
            long pLimit = unit.getLimit();
            if (pFree + size > pLimit) {
                allocBlockForUnit(size);
            }
            else {
                break;
            }
        }
        long pResult = pFree;
        unit.setFree(pFree + size);
        if (zero) {
            Unsafe.setMemory(pResult, size, (byte)0);
        }
        return pResult;
    }

    private void allocUnit(int size) {
        this.pUnit = createUnit(size) - Unit.OFFSET_END;
    }

    private void allocBlockForUnit(int size) {
        long pBlock = allocBlock(size);
        Unit unit = new Unit(this.pUnit);
        long pCurrentBlock = unit.getCurrentBlock();
        int blockSize = Unsafe.getInt(pBlock + OFFSET_SIZE);
        unit.setFree(pBlock + OFFSET_END);
        unit.setLimit(pBlock + blockSize);
        if (pCurrentBlock != 0) {
            Unsafe.putLong(pCurrentBlock + OFFSET_NEXT, pBlock);
        }
    }
    
    private long allocBlock(int size) {
        int blockSize = Math.max(this.unitSizeEstimate, size + OFFSET_END);
        blockSize = Integer.highestOneBit(blockSize - 1) << 1;
        int index = Integer.numberOfLeadingZeros(blockSize);
        long pBlock = this.blocks[index];
        if (pBlock == 0) {
            pBlock = this.parent.alloc(blockSize, false);
            Unsafe.putLong(pBlock + OFFSET_NEXT, 0);
            Unsafe.putInt(pBlock + OFFSET_SIZE,  blockSize);
            this.capacity += blockSize;
        }
        else {
            this.blocks[index] = Unsafe.getLong(pBlock + OFFSET_NEXT);
            Unsafe.putLong(pBlock + OFFSET_NEXT, 0);
            this.free -= blockSize;
        }
        this.usage += blockSize;
        return pBlock;
    }

    public void freeUnit(long p) {
        long pUnit2Free = p - Unit.OFFSET_END;
        Unit unit = new Unit(pUnit2Free);
        if (pUnit2Free == this.pUnit) {
            throw new IllegalArgumentException();
        }
        long pBlock = unit.getHeadBlock();
        while (pBlock != 0) {
            int blockSize = Unsafe.getInt(pBlock + OFFSET_SIZE);
            int index = Integer.numberOfLeadingZeros(blockSize);
            long pNextBlock = Unsafe.getLong(pBlock + OFFSET_NEXT);
            Unsafe.putLong(pBlock + OFFSET_NEXT, this.blocks[index]);
            this.blocks[index] = pBlock;
            pBlock = pNextBlock;
            this.free += blockSize;
            this.usage -= blockSize;
        }
    }
    
    @Override
    public void reset(long pos) {
        if (pos != 0) throw new IllegalArgumentException();
        this.parent.reset(this.parentPos);
    }

    @Override
    public long position() {
        throw new IllegalArgumentException();
    }

    @Override
    public void close() {
        if (this.freeParent) this.parent.close();
    }

    public long getCurrentUnit() {
        return this.pUnit != 0 ? this.pUnit + Unit.OFFSET_END : 0;
    }

    public long createUnit(int sizeEstimae) {
        long pBlock = allocBlock(sizeEstimae + Unit.OFFSET_END);
        long pResult = pBlock + OFFSET_END;
        Unit unit = new Unit(pResult);
        int blockSize = Unsafe.getInt(pBlock + OFFSET_SIZE);
        unit.setFree(pBlock + OFFSET_END + Unit.OFFSET_END);
        unit.setLimit(pBlock + blockSize);
        unit.setHeadBlock(pBlock);
        unit.setCurrentBlock(pBlock);
        return unit.getStart();
    }
    
    public void markNewUnit(int sizeEstimate) {
        this.unitSizeEstimate = sizeEstimate;
        this.pUnit = 0;
    }
    
    /**
     * get the total number of bytes managed by this heap including used and unused
     * 
     * @return
     */
    public long getSize() {
        return this.capacity;
    }
    
    static int getUnitSize(long p) {
        int result = 0;
        p = p - Unit.OFFSET_END - OFFSET_END;
        while (p != 0) {
            int unitSize = Unsafe.getInt(p + OFFSET_SIZE);
            result += unitSize;
            p = Unsafe.getLong(p + OFFSET_NEXT);
        }
        return result;
    }
    
    static long getNext(long p) {
        return Unsafe.getLong(p - 12 + OFFSET_NEXT);
    }

    public void restoreUnit(long pUnit2Restore) {
        this.pUnit = pUnit2Restore - Unit.OFFSET_END;
    }

    public Heap getParent() {
        return this.parent;
    }

    public long getUsage() {
        return this.usage;
    }
    
    public long getFree() {
        return this.free;
    }
    
    @Override
    public long getCapacity() {
        return this.capacity;
    }
}
