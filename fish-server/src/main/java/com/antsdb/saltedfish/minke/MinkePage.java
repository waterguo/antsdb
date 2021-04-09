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
package com.antsdb.saltedfish.minke;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.BluntHeap;
import com.antsdb.saltedfish.cpp.FileOffset;
import com.antsdb.saltedfish.cpp.FishSkipList;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.cpp.OutOfHeapException;
import com.antsdb.saltedfish.cpp.SkipListScanner;
import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.cpp.Value;
import com.antsdb.saltedfish.cpp.VariableLengthLongComparator;
import com.antsdb.saltedfish.nosql.IndexRow;
import com.antsdb.saltedfish.nosql.GetInfo;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.ScanOptions;
import com.antsdb.saltedfish.nosql.ScanResult;
import com.antsdb.saltedfish.nosql.TableType;
import com.antsdb.saltedfish.nosql.VaporizingRow;
import com.antsdb.saltedfish.util.CodingError;
import com.antsdb.saltedfish.util.UberFormatter;
import com.antsdb.saltedfish.util.UberTime;
import com.antsdb.saltedfish.util.UberUtil;

import static com.antsdb.saltedfish.util.UberFormatter.*;
import static com.antsdb.saltedfish.minke.BoundaryMark.*;

/**
 * 
 * @author *-xguo0<@
 */
public final class MinkePage implements Comparable<MinkePage> {
    final static int SIG = 0x73746e61;
    final static int HEADER_SIZE = 0x20;
    final static int OFFSET_SIG = 0;
    final static int OFFSET_USAGE = 4;
    final static int OFFSET_TABLE_ID = 8;
    final static int OFFSET_RANGE_LIST = 0xc;
    final static int OFFSET_STATE = 0x10;
    final static int OFFSET_START_KEY = 0x14;
    final static Logger _log = UberUtil.getThisLogger();
    
    private int size;
    long addr;
    private BluntHeap heap;
    int id;
    MinkeFile mfile;
    FishSkipList rows;
    FishSkipList ranges;
    AtomicLong hit = new AtomicLong();
    AtomicLong lastAccess = new AtomicLong();
    AtomicInteger waste = new AtomicInteger();
    Exception callstack = null;
    private KeyBytes keyStart;
    private KeyBytes keyEnd;
    volatile long pStartKey;
    private long pEndKey;
    long copyLastAccess;
    int tableId;
    private AtomicInteger state = new AtomicInteger(PageState.FREE);
    /** time when the page is put into garbage bin */
    long garbageTime;

    static enum ScanType {
        NULL,
        CACHE,
        STORAGE,
    }
    
    static class RangeData {
        static final int MASK_INCLUDE_START = 0x1;
        static final int MASK_INCLUDE_END = 0x2;
        static final int MASK_NULL_END = 0x4;
        
        static long alloc(Heap heap, Range range) {
            int size = 1;
            int keySize = 0; 
            if (range.pKeyEnd != 0) {
                keySize += KeyBytes.getRawSize(range.pKeyEnd);
                size += keySize;
            }
            long p = heap.alloc(size);
            if (keySize != 0) {
                Unsafe.copyMemory(range.pKeyEnd, p+1, keySize);
            }
            int flag = 0;
            if (range.startMark == BoundaryMark.NONE) {
                flag |= MASK_INCLUDE_START;
            }
            if (range.endMark == BoundaryMark.NONE) {
                flag |= MASK_INCLUDE_END;
            }
            Unsafe.putByte(p, (byte)flag);
            return p;
        }
        
        static Range create(long pKeyStart, long p) {
            if (p == 0) {
                return null;
            }
            Range result = new Range();
            int flag = Unsafe.getByte(p);
            result.pKeyStart = pKeyStart;
            result.pKeyEnd = p + 1;
            result.startMark = ((flag & MASK_INCLUDE_START) != 0) ? BoundaryMark.NONE : BoundaryMark.PLUS;
            result.endMark = ((flag & MASK_INCLUDE_END) != 0) ? BoundaryMark.NONE : BoundaryMark.MINUS;
            return result;
        }
    }
    
    class RangeScanner {
        SkipListScanner upstream;
        
        RangeScanner(SkipListScanner upsream) {
            this.upstream = upsream;
        }

        Range next() {
            if (this.upstream == null) {
                return null;
            }
            if (!this.upstream.next()) {
                return null;
            }
            long pRange = this.upstream.getValuePointer();
            Range range = toRange(pRange);
            return range;
        }
    }
    
    class Scanner extends ScanResult {

        SkipListScanner upstream;
        MinkePage mpage;
        long base;
        int counter = 0;
        long pRow;
        boolean skipDeletion = true;

        Scanner(SkipListScanner upstream, MinkePage page) {
            this.upstream = upstream;
            this.mpage = page;
            this.base = page.addr;
        }
        
        @Override
        public boolean next() {
            for (;;) {
                boolean hasNext = this.upstream.next();
                if (!hasNext) {
                    return false;
                }
                long pHead = this.upstream.getValuePointer();
                int oRow = Unsafe.getIntVolatile(pHead);
                if (oRow <= 1) {
                    if (this.skipDeletion) {
                        continue;
                    }
                    else {
                        this.pRow = oRow;
                    }
                }
                else {
                    this.pRow = this.base + oRow;
                }
                trackRead();
                this.counter++;
                return true;
            }
        }

        @Override
        public long getIndexRowKeyPointer() {
            long pValue = this.upstream.getValuePointer();
            int oRowKey = Unsafe.getInt(pValue);
            if (oRowKey <= 1) {
                return oRowKey;
            }
            long pIndex = this.base + oRowKey;
            return new IndexRow(pIndex).getRowKeyAddress();
        }

        @Override
        public long getIndexSuffix() {
            long pKey = getKeyPointer();
            if (pKey == 0) {
                return 0;
            }
            long suffix = KeyBytes.create(pKey).getSuffix();
            return suffix;
        }

        @Override
        public boolean eof() {
            return this.upstream.eof();
        }

        @Override
        public long getRowPointer() {
            return this.pRow;
        }

        @Override
        public long getKeyPointer() {
            return this.upstream.getKeyPointer(); 
        }

        @Override
        public byte getMisc() {
            long pRowKey = getIndexRowKeyPointer();
            if (pRowKey == 0) {
                return 0;
            }
            return KeyBytes.create(pRowKey).getSuffixByte();
        }

        public long getRowScanned() {
            return this.counter;
        }

        @Override
        public void rewind() {
            throw new NotImplementedException();
        }

        @Override
        public void close() {
        }

        @Override
        public long getVersion() {
            long pRow = getRowPointer();
            if (pRow < 10) {
                return 0;
            }
            switch (Value.getFormat(null, this.pRow)) {
            case Value.FORMAT_ROW:
                return Row.getVersion(this.pRow);
            case Value.FORMAT_INDEX_ROW:
                return new IndexRow(this.pRow).getVersion();
            default:
                throw new IllegalArgumentException();
            }
        }

        @Override
        public String toString() {
            return getLocation();
        }
        
        MinkePage getPage() {
            return this.mpage;
        }
        
        long getData() {
            return this.pRow;
        }

        @Override
        public String getLocation() {
            String result = getRowLocation(pRow, getKeyPointer());
            return result;
        }
    }
    
    MinkePage(MinkeFile cacheFile, long addr, int size, int id) {
        if (id == 0) {
            throw new CodingError("page id cannot be 0");
        }
        this.addr = addr;
        this.size = size;
        this.mfile = cacheFile;
        this.id = id;
        if (getSignature() != SIG) {
            heap = new BluntHeap(addr, this.size);
            heap.alloc(HEADER_SIZE);
            Unsafe.putInt(this.addr + OFFSET_SIG, SIG);
            this.rows = FishSkipList.alloc(heap, new VariableLengthLongComparator());
            this.ranges = FishSkipList.alloc(this.heap, KeyBytes.getComparator());
            Unsafe.putIntVolatile(this.addr + OFFSET_RANGE_LIST, (int)(this.ranges.getAddress() - this.addr));
        }
        else {
            this.rows = new FishSkipList(this.addr, HEADER_SIZE, new VariableLengthLongComparator());
            int offset = Unsafe.getIntVolatile(this.addr + OFFSET_RANGE_LIST);
            this.ranges = new FishSkipList(this.addr, offset, KeyBytes.getComparator());
        }
    }
    
    /** 
     * reset the page so it can be reused
     */
    void reset() {
        if (getState() != PageState.FREE) {
            throw new IllegalArgumentException(UberFormatter.hex(this.id));
        }
        this.state.addAndGet(PageState.FREE);
        heap = new BluntHeap(addr, this.size);
        heap.alloc(HEADER_SIZE);
        Unsafe.putInt(this.addr + OFFSET_SIG, SIG);
        Unsafe.putInt(this.addr + OFFSET_TABLE_ID, 0);
        setSavedState(PageState.FREE);
        setUsage(0);
        this.rows = FishSkipList.alloc(heap, KeyBytes.getComparator());
        this.ranges = FishSkipList.alloc(this.heap, KeyBytes.getComparator());
        Unsafe.putIntVolatile(this.addr + OFFSET_RANGE_LIST, (int)(this.ranges.getAddress() - this.addr));
        this.pStartKey = 0;
        this.pEndKey = 0;
        this.keyStart = null;
        this.keyEnd = null;
        this.hit.set(0);
        this.lastAccess.set(0);
        this.callstack = null;
        this.garbageTime = 0;
        this.waste.set(0);
    }
    
    int getSignature() {
        return Unsafe.getInt(this.addr + OFFSET_SIG);
    }
    
    long put(VaporizingRow row) {
        try {
            long pKey = row.getKeyAddress();
            ensureMutable(pKey);
            long pRow = Row.from(heap, row);
            for (;;) {
                long pHead = this.rows.put(pKey);
                int oHeadValue = Unsafe.getIntVolatile(pHead);
                int oRow = (int)(pRow - this.addr);
                if (Unsafe.compareAndSwapInt(pHead, oHeadValue, oRow)) {
                    trackWrite(oHeadValue);
                    break;
                }
            }
            return pRow;
        }
        finally {
        }
    }

    public long get(long pKey, GetInfo info) {
        return get(pKey, true, info);
    }
    
    public long get(long pKey, boolean track, GetInfo info) {
        long result = 0;
        long pHead = this.rows.get(pKey);
        if (pHead != 0) {
            int oRow = Unsafe.getIntVolatile(pHead);
            if (track) {
                trackRead();
            }
            if (oRow <= 1) {
                return oRow;
            }
            result = this.addr + oRow;
            if (info != null) {
                info.pData = result;
                info.version = Row.getVersion(result);
                info.location = getLocation(oRow);
            }
        }
        return result;
    }

    private void ensureMutable(long pKey) {
        if (KeyBytes.compare(pKey, getEndKeyPointer()) >= 0) {
            throw new OutOfPageRange(KeyBytes.toString(pKey) + "," + KeyBytes.toString(getEndKeyPointer()));
        }
        if (KeyBytes.compare(pKey, getStartKeyPointer()) < 0) {
            throw new OutOfPageRange(KeyBytes.toString(pKey) + "," + KeyBytes.toString(getStartKeyPointer()));
        }
        if (this.heap == null) {
            // this is a carbonfreezed page. need to split
            throw new OutOfHeapException();
        }
        if (pKey == 0) {
            return;
        }
    }

    long put(Row row) {
        try {
            long pKey = row.getKeyAddress();
            ensureMutable(pKey);
            int oNewRow = row.clone(heap);
            for (;;) {
                long pHead = this.rows.put(pKey);
                int oHeadValue = Unsafe.getIntVolatile(pHead);
                if (Unsafe.compareAndSwapInt(pHead, oHeadValue, oNewRow)) {
                    trackWrite(oHeadValue);
                    break;
                }
            }
            return heap.getAddress(oNewRow);
        }
        finally {
        }
    }

    void delete(long pKey) {
        delete_(pKey, 0);
    }

    void putDeleteMark(long pKey) {
        delete_(pKey, Row.DELETE_MARK);
    }
    
    private void delete_(long pKey, int value) {
        try {
            ensureMutable(pKey);
            for (;;) {
                long pHead = this.rows.put(pKey);
                int oHeadValue = Unsafe.getIntVolatile(pHead);
                if (Unsafe.compareAndSwapInt(pHead, oHeadValue, value)) {
                    trackWrite(oHeadValue);
                    break;
                }
            }
        }
        finally {
        }
    }
    
    public long putIndex(long version, long pIndexKey, long pRowKey, byte misc) {
        try {
            ensureMutable(pIndexKey);
            IndexRow index = IndexRow.alloc(this.heap, version, pRowKey, misc);
            int oNewRowKey = (int) (index.getAddress() - this.addr);
            for (;;) {
                long pHead = this.rows.put(pIndexKey);
                int oHeadValue = Unsafe.getIntVolatile(pHead);
                if (Unsafe.compareAndSwapInt(pHead, oHeadValue, oNewRowKey)) {
                    trackWrite(oHeadValue);
                    return index.getRowKeyAddress();
                }
            }
        }
        finally {
        }
    }

    public long getAddress() {
        return this.addr;
    }

    public String getCoverage() {
        return KeyBytes.toString(this.pStartKey) + '-' + KeyBytes.toString(this.pEndKey);
    }
    
    @Override
    public String toString() {
        return String.format(
                "%s:%s (%s - %s)", 
                this.mfile, UberFormatter.hex(this.id), 
                KeyBytes.toString(this.pStartKey),
                KeyBytes.toString(this.pEndKey));
    }

    Scanner scan(
            long pKeyStart, 
            boolean includeStart, 
            long pKeyEnd, 
            boolean includeEnd,
            boolean isAscending) {
        SkipListScanner upstream = null; 
        if (isAscending) {
            upstream = this.rows.scan(pKeyStart, includeStart, pKeyEnd, includeEnd);
        }
        else {
            upstream = this.rows.scanReverse(pKeyStart, includeStart, pKeyEnd, includeEnd);
        }
        if (upstream == null) {
            return null;
        }
        Scanner scanner = new Scanner(upstream, this);
        return scanner;
    }

    public Scanner scan(Range range, boolean isAscending) {
        if (isAscending) {
            Scanner result = scan(
                    range.pKeyStart, 
                    (range.startMark == NONE) ? true : false,
                    range.pKeyEnd,
                    (range.endMark == NONE) ? true : false,
                    isAscending);
            return result;
        }
        else {
            Scanner result = scan(
                    range.pKeyEnd, 
                    (range.endMark == NONE) ? true : false,
                    range.pKeyStart,
                    (range.startMark == NONE) ? true : false,
                    isAscending);
            return result;
        }
    }
    
    public Scanner scanAll(long options) {
        SkipListScanner upstream = this.rows.scan(0, true, 0, true);
        if (upstream == null) {
            return null;
        }
        Scanner scanner = new Scanner(upstream, this);
        scanner.skipDeletion = !ScanOptions.isShowDeleteMarkOn(options);
        return scanner;
    }

    public float getUsageRatio() {
        float result = (this.getUsage()) / (float)this.size;
        return result;
    }
    
    public int getSavedTableId() {
        return Unsafe.getIntVolatile(this.addr + OFFSET_TABLE_ID);
    }
    
    private void setSavedTableId(int parentPage) {
        Unsafe.putIntVolatile(this.addr + OFFSET_TABLE_ID, parentPage);
    }
    
    private void setSavedState(int value) {
        Unsafe.putIntVolatile(this.addr + OFFSET_STATE, value);
    }
    
    public int getSavedState() {
        return Unsafe.getIntVolatile(this.addr + OFFSET_STATE);
    }
    
    private void setSavedStartKey(long pKey) {
        Unsafe.putIntVolatile(this.addr + OFFSET_START_KEY, (int)(pKey - this.addr));
    }
    
    public long getSavedStartKey() {
        return this.addr + Unsafe.getIntVolatile(this.addr + OFFSET_START_KEY);
    }
    
    private int getState(int value) {
        int result = value & 0x7;
        return result;
    }
    
    public int getState() {
        return this.state.get() & 0x7;
    }
    
    private int getNewState(int oldState, int newState) {
        int result = oldState & ~0x7 | newState;
        return result;
    }

    /**
     * set page state to zombie
     */
    public synchronized void zombie() {
        int current = this.state.get();
        int currentState = getState(current);
        if (currentState == PageState.CARBONFREEZED) {
            this.state.compareAndSet(current, getNewState(currentState, PageState.ZOMBIE));
            return;
        }
        else {
            String msg = String.format("page=%d state=%d", this.id, current);
            throw new IllegalArgumentException(msg);
        }
    }
    
    public synchronized void carbonfreeze() throws IOException {
        int current = this.state.get();
        int currentState = getState(current);
        if (currentState == PageState.CARBONFREEZED) {
            return;
        }
        if (currentState != PageState.ACTIVE) {
            throw new IllegalArgumentException(UberFormatter.hex(this.id));
        }
        this.state.compareAndSet(current, getNewState(currentState, PageState.CARBONFREEZED));
        setSavedState(PageState.CARBONFREEZED);
        freeze();
        this.mfile.force(this);
    }

    public long getStartKeyPointer() {
        return pStartKey;
    }

    public KeyBytes getStartKey() {
        return this.keyStart;
    }
    
    public long getEndKeyPointer() {
        return this.pEndKey;
    }
    
    void assign(int tableId, KeyBytes startKey, KeyBytes endKey) {
        int current = this.state.get();
        if (getState(current) != PageState.FREE) {
            throw new IllegalArgumentException(UberFormatter.hex(this.id));
        }
        int newState = (this.heap == null) ? PageState.CARBONFREEZED : PageState.ACTIVE;
        if (!this.state.compareAndSet(current, getNewState(current, newState))) {
            throw new IllegalArgumentException(UberFormatter.hex(this.id));
        }
        this.keyStart = startKey;
        this.pStartKey = startKey.getAddress();
        this.keyEnd = endKey;
        this.pEndKey = (endKey == null) ? 0 : endKey.getAddress();
        this.tableId = tableId;
        setSavedTableId(tableId);
        setSavedState(PageState.ACTIVE);
        setSavedStartKey(KeyBytes.alloc(this.pStartKey).getAddress());
        _log.debug("page {} is assigned with {}-{} tableId={}", 
                hex(this.id), 
                KeyBytes.toString(this.pStartKey),
                KeyBytes.toString(this.pEndKey),
                this.tableId);
    }
    
    void assign(int tableId, long pKey, long pEndKey) {
        if (KeyBytes.compare(pKey, pEndKey) >= 0) {
            throw new IllegalArgumentException(KeyBytes.toString(pKey) + "," + KeyBytes.toString(pEndKey));
        }
        KeyBytes start = KeyBytes.alloc(pKey);
        KeyBytes end = KeyBytes.alloc(pEndKey);
        assign(tableId, start, end);
    }
    
    long getTailKeyPointer() {
        return this.rows.getTailKeyPointer();
    }
    
    long getTail() {
        return this.rows.getTail();
    }

    /**
     * find the range that intersects with the input
     * 
     * @param request
     */
    public ScanType findRange(Range request, Range result) {
        ScanType type = findRange_(request, result);
        if ((type != ScanType.NULL) && !result.isValid()) {
            throw new IllegalArgumentException();
        }
        return type;
    }
    
    public Range findRange(Boundary boundary) {
        if (ranges == null) {
            return null;
        }
        long pRange = this.ranges.floor(boundary.pKey);
        if (pRange == 0) {
            return null;
        }
        Range range = toRange(pRange);
        if (range == null) {
            return null;
        }
        Range result = range.contains(boundary) ? range : null;
        if (result != null) {
            trackRead();
        }
        return result;
    }
    
    private ScanType findRange_(Range request, Range result) {
        /* suppose incoming range is AB, matched range is CD
         * 
         * ABCD: return 0
         * ACBD: return AC-
         * ACDB: return AC-
         * CABD: return AB
         * CADB: return AD
         * CDAB: return 0
         */
        if (this.ranges == null) {
            return ScanType.NULL;
        }
        Boundary start = request.getStart();
        Boundary end = request.getEnd();
        long pFloorRange = this.ranges.floor(request.pKeyStart);
        if (pFloorRange != 0) {
            // C???
            Range floorRange = toRange(pFloorRange);
            if (floorRange == null) {
                return ScanType.NULL;
            }
            if (!floorRange.isGreater(start)) {
                result.pKeyStart = request.pKeyStart;
                result.startMark = request.startMark;
                if (floorRange.isGreater(end)) {
                    // CADB
                    result.pKeyEnd = floorRange.pKeyEnd;
                    result.endMark = floorRange.endMark;
                }
                else {
                    // CABD
                    result.pKeyEnd = request.pKeyEnd;
                    result.endMark = request.endMark;
                }
                return ScanType.CACHE;
            }
            // CDAB...... try next range
        }
        long pHigherRange = this.ranges.higher(request.pKeyStart);
        if (pHigherRange != 0) {
            Range higherRange = toRange(pFloorRange);
            // A???
            result.pKeyStart = request.pKeyStart;
            result.startMark = request.startMark;
            if (request.isGreater(higherRange.getStart())) {
                // ABCD
                result.pKeyEnd = request.pKeyEnd;
                result.endMark = request.endMark;
            }
            else {
                // ACBD ACDB
                result.pKeyEnd = higherRange.pKeyStart;
                result.endMark = higherRange.startMark - 1;
                return ScanType.STORAGE;
            }
        }
        return ScanType.NULL;
    }

    Range toRange(long pRange) {
        if (pRange == 0) {
            return null;
        }
        FishSkipList.Entry entry = new FishSkipList.Entry(pRange);
        long p = entry.getValuePointer();
        int offset = Unsafe.getIntVolatile(p);
        long pKeyEnd = this.addr + offset;
        Range result = RangeData.create(entry.getKeyPointer(), pKeyEnd);
        if (Range.compare(result.pKeyEnd, result.endMark, this.pEndKey, MINUS) > 0) {
            if (Range.compare(result.pKeyStart, result.endMark, this.pEndKey, MINUS) >= 0) {
                return null;
            }
            result.pKeyEnd = this.pEndKey;
            result.endMark = MINUS;
        }
        return result;
    }

    public synchronized long putRange(Range value) {
        if (!value.isValid() || value.isEmpty()) {
            throw new IllegalArgumentException();
        }
        try {
            /*
             * suppose incoming range is f, starting f1 ending f2
             *          f1 ------------------- f2
             *    x1----------x2 ........  y1---------y2
             * we need to find start range x and end range y
             * the merged range z = x1--------y2
             */
            ensureMutable(value.pKeyStart);
            Range start = findStartRange(this.ranges, value);
            Range end = findEndRange(this.ranges, value);
            Range merged = merge(start, end, value);
            long result = putRange(this.ranges, merged);
            deleteObsoleteRanges(this.ranges, merged);
            trackWrite(0);
            return result;
        }
        finally {
        }
    }

    private void deleteObsoleteRanges(FishSkipList slist, Range merged) {
        SkipListScanner scanner = slist.scan(merged.pKeyStart, false, merged.pKeyEnd, true);
        if (scanner == null) {
            return;
        }
        while (scanner.next()) {
            long pKey = scanner.getKeyPointer();
            slist.delete(pKey);
        }
    }

    private Range findEndRange(FishSkipList slist, Range value) {
        long pRange =slist.floor(value.pKeyEnd);
        if (pRange == 0) {
            return null;
        }
        Range result = toRange(pRange);
        if (result == null) {
            return null;
        }
        if (result.pKeyEnd == Long.MIN_VALUE) {
            return result;
        }
        if (KeyBytes.compare(result.pKeyEnd, value.pKeyEnd) >= 0) {
            return result;
        }
        else {
            return null;
        }
    }

    private Range findStartRange(FishSkipList slist, Range value) {
        long pRange = slist.floor(value.pKeyStart);
        if (pRange == 0) {
            return null;
        }
        Range result = toRange(pRange);
        if (result == null) {
            return null;
        }
        if (result.pKeyEnd == Long.MIN_VALUE) {
            return result;
        }
        if (KeyBytes.compare(result.pKeyEnd, value.pKeyStart) >= 0) {
            return result;
        }
        else {
            return null;
        }
    }

    private Range merge(Range start, Range end, Range value) {
        if ((start == null) && (end == null)) {
            return value;
        }
        Range result = new Range();
        if (start == null) {
            result.pKeyStart = value.pKeyStart;
            result.startMark = value.startMark;
        }
        else {
            result.pKeyStart = start.pKeyStart;
            result.startMark = start.startMark;
        }
        if (end == null) {
            result.pKeyEnd = value.pKeyEnd;
            result.endMark = value.endMark;
        }
        else {
            result.pKeyEnd = end.pKeyEnd;
            result.endMark = end.endMark;
        }
        return result;
    }

    private long putRange(FishSkipList slist, Range range) {
        long pRange = RangeData.alloc(heap, range);
        long pValue = slist.put(range.pKeyStart);
        Unsafe.putIntVolatile(pValue, (int)(pRange - this.addr));
        return pRange;
    }

    /**
     * free the page so that mutation is not allowed. 
     */
    public void freeze() {
        if (this.heap != null) {
            int usage = this.heap.freeze();
            setUsage(usage);
        }
    }

    public void setEndKey(long pKey) {
        this.keyEnd = KeyBytes.alloc(pKey);
        this.pEndKey = this.keyEnd.getAddress();
        _log.debug("change end key of page {} to {}", hex(this.id), KeyBytes.toString(pKey));
    }

    public KeyBytes getEndKey() {
        return this.keyEnd;
    }

    public Range getRange() {
        Range result = new Range();
        result.pKeyStart = this.pStartKey;
        result.startMark = BoundaryMark.NONE;
        result.pKeyEnd = this.pEndKey;
        result.endMark = BoundaryMark.MINUS;
        return result;
    }

    void splitRanges(MinkePage page1, MinkePage page2) {
        if (this.ranges == null) {
            return;
        }
        SkipListScanner scanner = this.ranges.scan(KeyBytes.getMinKey(), true, KeyBytes.getMaxKey(), true);
        if (scanner == null) {
            return;
        }
        Range range1 = page1.getRange();
        Range range2 = page2.getRange();
        while (scanner.next()) {
            long pRange = scanner.getValuePointer();
            Range range = toRange(pRange);
            if (range == null) {
                continue;
            }
            if (range.in(range1)) {
                page1.putRange(range);
            }
            else if (range.in(range2)) {
                page2.putRange(range);
            }
            else {
                Range x = range1.intersect(range);
                Range y = range2.intersect(range);
                page1.putRange(x);
                page2.putRange(y);
            }
        }
    }

    RangeScanner getAllRanges() {
        if (this.ranges == null) {
            return new RangeScanner(null);
        }
        return new RangeScanner(this.ranges.scan(KeyBytes.getMinKey(), true, KeyBytes.getMaxKey(), true));
    }
    
    void copyRanges(MinkePage that) {
        RangeScanner scanner = getAllRanges();
        if (scanner == null) {
            return;
        }
        for (;;) {
            Range range = scanner.next();
            if (range == null) {
                break;
            }
            that.putRange(range);
        }
    }

    public ScanResult findNextRangeAndDoShit(Range x, FindNextRangeAndDoShitCallback callback, boolean ascending) {
        if (this.ranges == null) {
            return null;
        }
        if (ascending) {
            Range range = findRange(new Boundary(x.pKeyStart, x.startMark));
            if (range != null) {
                Range intersection = range.intersect(x);
                return callback.doShit(this, intersection, false, ascending);
            }
            long pRange = this.ranges.higher(x.pKeyStart);
            range = toRange(pRange);
            if (range == null) {
                return null;
            }
            if (range.hasIntersection(x)) {
                Range intersection = new Range();
                intersection.pKeyStart = x.pKeyStart;
                intersection.startMark = x.startMark;
                intersection.pKeyEnd = range.pKeyStart;
                intersection.endMark = range.startMark - 1;
                return callback.doShit(this, intersection, true, ascending);
            }
            return null;
        }
        else {
            long pRange = this.ranges.floor(x.pKeyEnd);
            if (pRange == 0) {
                return null;
            }
            Range range = toRange(pRange);
            if (x.getEnd().in(range)) {
                Range intersection = range.intersect(x);
                return callback.doShit(this, intersection, false, ascending);
            }
            if (x.getStart().in(range)) {
                Range intersection = new Range();
                intersection.pKeyStart = range.pKeyEnd;
                intersection.startMark = range.endMark + 1;
                intersection.pKeyEnd = x.pKeyEnd;
                intersection.endMark = x.endMark;
                return callback.doShit(this, intersection, true, ascending);
            }
            return null;
        }
    }
    
    private void trackRead() {
        this.hit.lazySet(this.hit.get()+1);
        this.lastAccess.lazySet(UberTime.getTime());
    }

    private void trackWrite(int oOldObjectOffset) {
        this.lastAccess.lazySet(UberTime.getTime());
        
        // calculate wasted bytes - bytes belongs to deleted/replaced data
        if (oOldObjectOffset >= HEADER_SIZE) {
            long pOld = this.addr + oOldObjectOffset;
            byte format = Value.getFormat(this.heap, pOld);
            int size = 0;
            if (format == Value.FORMAT_ROW) {
                size = Row.getLength(pOld);
            }
            else if (format == Value.FORMAT_KEY_BYTES) {
                size = KeyBytes.getRawSize(pOld);
            }
            else if (format == Value.FORMAT_INDEX_ROW) {
                size = new IndexRow(pOld).getSize();
            }
            else {
                _log.warn("unknown format {}", format);
            }
            this.waste.addAndGet(size);
        }
    }

    public boolean free() {
        int current = this.state.get();
        int currentState = getState(current);
        switch (currentState) {
            case PageState.ACTIVE:
            case PageState.CARBONFREEZED:
            case PageState.GARBAGE:
                boolean result = this.state.compareAndSet(current, getNewState(current, PageState.FREE));
                return result;
            default:
                return false;
        }
    }

    public boolean garbage() {
        int current = this.state.get();
        int currentState = getState(current);
        switch (currentState) {
            case PageState.ACTIVE:
            case PageState.CARBONFREEZED:
            case PageState.ZOMBIE:
                boolean result = this.state.compareAndSet(current, getNewState(current, PageState.GARBAGE));
                if (result) {
                    this.garbageTime = UberTime.getTime();
                }
                return result;
            default:
                return false;
        }
    }
    
    private void setUsage(int value) {
        Unsafe.putIntVolatile(this.addr + OFFSET_USAGE, value);
    }

    /**
     * get the usage of the page in number of bytes.
     * 
     * @return
     */
    public int getUsage() {
        if (this.heap != null) {
            int result = (int)this.heap.position();
            return result;
        }
        int result = Unsafe.getIntVolatile(this.addr + OFFSET_USAGE);
        return result;
    }
    
    long put(TableType type, long pKey, long pRow, long version) {
        if (pRow == 0) {
            delete(pKey);
            return 0;
        }
        else if (pRow == Row.DELETE_MARK) {
            putDeleteMark(pKey);
            return 0;
        }
        if (type == TableType.DATA) {
            Row row = Row.fromMemoryPointer(pRow, version);
            if (row != null) {
                return put(row);
            }
            else {
                throw new IllegalArgumentException();
            }
        }
        else {
            IndexRow index = new IndexRow(pRow);
            return putIndex(version, pKey, index.getRowKeyAddress(), index.getMisc());
        }
    }
    
    void put(TableType type, ScanResult source) {
        long pRow = source.getRowPointer();
        long pKey = source.getKeyPointer();
        if (pRow == 0) {
            return;
        }
        else if (pRow == Row.DELETE_MARK) {
            putDeleteMark(pKey);
            return;
        }
        if (type == TableType.DATA) {
            long version = source.getVersion();
            Row row = Row.fromMemoryPointer(pRow, version);
            if (row != null) {
                put(row);
            }
        }
        else {
            long pRowKey = source.getIndexRowKeyPointer();
            if (pRowKey != 0) {
                byte misc = source.getMisc();
                putIndex(source.getVersion(), pKey, pRowKey, misc);
            }
        }
    }

    @Override
    public int compareTo(MinkePage that) {
        return Integer.compare(this.id, that.id);
    }

    @Override
    public boolean equals(Object that) {
        return this == that;
    }

    public Range findHigherRange(long pKey, int mark) {
        if (mark <= NONE) {
            long pRange = this.ranges.floor(pKey);
            if (pRange != 0) {
                Range range = toRange(pRange);
                if (Range.compare(range.pKeyStart, range.startMark, pKey, mark) > 0) {
                    return range;
                }; 
            }
        }
        long pRange = this.ranges.higher(pKey);
        Range range = toRange(pRange);
        if (range == null) {
            return null;
        }
        return (Range.compare(range.pKeyStart, range.startMark, pKey, mark) > 0) ? range : null;
    }
    
    public Range findCeilingRange(long pKey, int mark) {
        long pRange = this.ranges.floor(pKey);
        if (pRange != 0) {
            Range range = toRange(pRange);
            // is the position in the range
            if (Range.compare(pKey, mark, range.pKeyEnd, range.endMark) <= 0) {
                return range;
            }
        }
        pRange = this.ranges.higher(pKey);
        return toRange(pRange);
    }

    public int getTableId() {
        return this.tableId;
    }
    
    public int getId() {
        return this.id;
    }

    public String getLocation(long pKey) {
        long pHead = this.rows.get(pKey);
        if (pHead == 0) {
            return null;
        }
        int oRow = Unsafe.getIntVolatile(pHead);
        long pRow = this.addr + oRow;
        String result = getRowLocation(pRow, pKey);
        return result;
    }
    
    private String getLocation(int offset) {
        File file = this.mfile.file;
        Long offsetFile = offset + this.addr - this.mfile.addr;
        String result = String.format(
                "%s:%s/%s:%s", 
                hex(this.id),
                hex(offset),
                file.getName(), 
                hex(offsetFile));
        return result;
    }
    
    private String getRowLocation(long pRow, long pKey) {
        File file = this.mfile.file;
        Long offsetFile = (pRow != 0) ? pRow - this.mfile.addr : null;
        Long offsetPage = (pRow != 0) ? pRow - this.addr : null;
        String result = String.format(
                "%s:%s/%s:%s", 
                hex(this.id),
                hex(offsetPage),
                file.getName(), 
                hex(offsetFile));
        return result;
    }
    
    public long getHits() {
        return this.hit.get();
    }

    public long getLastRead() {
        return this.lastAccess.get();
    }

    public boolean traceIo(long pKey, List<FileOffset> lines) {
        long pHead = this.rows.traceIo(pKey, this.mfile.file, this.mfile.addr, lines);
        if (pHead == 0) {
            return false;
        }
        int oRow = Unsafe.getIntVolatile(pHead);
        if (oRow <= 1) {
            return false;
        }
        long pageOffset = this.addr - this.mfile.addr;
        long offset = pageOffset + oRow;
        lines.add(new FileOffset(this.mfile.file, offset, "row"));
        return true;
    }

    public boolean isFrozen() {
        return getState() != PageState.ACTIVE;
    }

    public int getWastedBytes() {
        return this.waste.get();
    }
}
