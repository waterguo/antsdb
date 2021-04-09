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

import io.netty.util.internal.ThreadLocalRandom;

/**
 * a skip list implementation using off heap memory
 * 
 * @author *-xguo0<@
 */
public final class OffHeapSkipList {
    static final long KEY_NULL = Unsafe.allocateMemory(8);
    static final int OFFSET_SIG = 0;
    static final int OFFSET_ROOT = 2;
    static final int OFFSET_HEAD = 0xa;
    static final int OFFSET_END = 0x12;
    static final int OP_EQUAL = 0;
    static final int OP_LT = 1;
    static final int OP_GT = 2;
    static final int OP_GE = 3;
    static final int OP_LE = 4;

    static final Logger _log = UberUtil.getThisLogger();
    static final VariableLengthLongComparator _comp = new VariableLengthLongComparator();
    
    private KeyComparator comparator = _comp;
    Heap heap;
    private long addr;
    
    final static class Node {
        private final static int OFFSET_VALUE = 0;
        private final static int OFFSET_NEXT = 8;
        private final static int OFFSET_KEY = 0x10;

        final static long alloc(Heap heap, long pKey, long oNext ) {
            int size = (pKey != 0) ? KeyBytes.getRawSize(pKey) : 0;
            long pResult = heap.alloc(OFFSET_KEY + size);
            setValue(pResult, 0);
            setNext(pResult, oNext);
            Unsafe.copyMemory(pKey, pResult + OFFSET_KEY, size);
            return pResult;
        }
        
        public final static long getNext(long pAddr) {
            long result = Unsafe.getLongVolatile(pAddr + OFFSET_NEXT);
            return result;
        }

        public static void setNext(long pAddr, long oNext) {
            Unsafe.putLongVolatile(pAddr + OFFSET_NEXT, oNext);
        }

        @SuppressWarnings("unused")
        public final static boolean casValue(long pAddr, long oldValue, long newValue) {
            return Unsafe.compareAndSwapLong(pAddr + OFFSET_VALUE, oldValue, newValue);
        }

        final static long getValue(long pAddr) {
            return Unsafe.getLongVolatile(pAddr + OFFSET_VALUE);
        }
        
        public final static void setValue(long pAddr, int pValue) {
            Unsafe.putIntVolatile(pAddr + OFFSET_VALUE, pValue);
        }

        public final static boolean casNext(long pAddr, long oldValue, long newValue) {
            return Unsafe.compareAndSwapLong(pAddr + OFFSET_NEXT, oldValue, newValue);
        }

        public final static long getKeyPointer(long pAddr) {
            return pAddr + OFFSET_KEY;
        }

        @SuppressWarnings("unused")
        public final static long getValueOffset(long pAddr) {
            return pAddr + OFFSET_VALUE;
        }
        
        public final static long getValuePointer(long pAddr) {
            long p = pAddr + OFFSET_VALUE;
            return p;
        }

        public final static boolean isDeleted(long oNode) {
            return false;
        }
            
        @SuppressWarnings("unused")
        public static Object toString(long pAddr) {
            StringBuilder buf = new StringBuilder();
            buf.append("ND-");
            buf.append(String.format("%08x", pAddr));
            buf.append(" [");
            buf.append("value:");
            buf.append(getValue(pAddr));
            buf.append(" next:");
            buf.append(getNext(pAddr));
            buf.append(" key:");
            buf.append(KeyBytes.toString(getKeyPointer(pAddr)));
            buf.append(']');
            return buf.toString();
        }
    }
    
    private static class IndexNode {
        protected final static int OFFSET_RIGHT = 0;
        protected final static int OFFSET_DOWN = 8;
        protected final static int OFFSET_NODE = 0x10;
        protected final static int OFFSET_END = 0x18;
        
        final static long alloc(Heap heap) {
            return heap.alloc(OFFSET_END);
        }
        
        final static long alloc(Heap heap, long node , long down, long right) {
            long pResult = alloc(heap);
            Unsafe.putLongVolatile(pResult + OFFSET_RIGHT, right);
            Unsafe.putLongVolatile(pResult + OFFSET_DOWN, down);
            Unsafe.putLongVolatile(pResult + OFFSET_NODE, node);
            return pResult;
        }
        
        final static long getRight(long pAddr) {
            long blah = pAddr + OFFSET_RIGHT;
            return Unsafe.getLongVolatile(blah);
        }
        
        final static void setRight(long oIndexNode, long right) {
            Unsafe.putLongVolatile(oIndexNode + OFFSET_RIGHT, right);
        }
        
        final static boolean casRight(long oIndexNode, long oldRight, long newRight) {
            return Unsafe.compareAndSwapLong(oIndexNode + OFFSET_RIGHT, oldRight, newRight);
        }
        
        final static long getDown(long pAddr) {
            return Unsafe.getLongVolatile(pAddr + OFFSET_DOWN);
        }
        
        final static long getNode(long pAddr) {
            return Unsafe.getLongVolatile(pAddr + OFFSET_NODE);
        }

        final static boolean link(long oIndexNode, long oOldRight, long oNewRight) {
            setRight(oNewRight, oOldRight);
            return casRight(oIndexNode, oOldRight, oNewRight);
        }

        @SuppressWarnings("unused")
        public static Object toString(long pAddr) {
            StringBuilder buf = new StringBuilder();
            buf.append("IN-");
            buf.append(String.format("%04x", pAddr));
            buf.append(" [");
            buf.append("right:");
            buf.append(IndexNode.getRight(pAddr));
            buf.append(" down:");
            buf.append(IndexNode.getDown(pAddr));
            buf.append(" node:");
            buf.append(IndexNode.getNode(pAddr));
            buf.append(']');
            return buf.toString();
        }
    }
    
    private final static class HeadIndex extends IndexNode {
        protected final static int OFFSET_LEVEL_OFFSET = OFFSET_END;
        
        final static long alloc(Heap heap, long node, long down, long right, int level) {
            long pResult = heap.alloc(OFFSET_END + 4);
            Unsafe.putLongVolatile(pResult + OFFSET_RIGHT, right);
            Unsafe.putLongVolatile(pResult + OFFSET_DOWN, down);
            Unsafe.putLongVolatile(pResult + OFFSET_NODE, node);
            Unsafe.putLongVolatile(pResult + OFFSET_LEVEL_OFFSET, level);
            return pResult;
        }
        
        final static int getLevel(long pAddr) {
            return Unsafe.getIntVolatile(pAddr + OFFSET_LEVEL_OFFSET);
        }

        public static Object toString(long pAddr) {
            StringBuilder buf = new StringBuilder();
            buf.append("HI-");
            buf.append(String.format("%08x", pAddr));
            buf.append(" [");
            buf.append("level:");
            buf.append(getLevel(pAddr));
            buf.append(" right:");
            buf.append(IndexNode.getRight(pAddr));
            buf.append(" down:");
            buf.append(IndexNode.getDown(pAddr));
            buf.append(" node:");
            buf.append(IndexNode.getNode(pAddr));
            buf.append(']');
            return buf.toString();
        }
    }
    
    public OffHeapSkipList(Heap heap, long addr) {
        if (Unsafe.getShort(addr) != 0x7777) {
            throw new IllegalArgumentException();
        }
        this.heap = heap;
        this.addr = addr;
    }
    
    public static OffHeapSkipList alloc(Heap heap) {
        long addr = heap.alloc(2);
        Unsafe.putShort(addr, (short)0x7777);
        heap.alloc(OffHeapSkipList.OFFSET_END);
        OffHeapSkipList sl = new OffHeapSkipList(heap, addr);
        sl.heap = heap;
        sl.setHead(Node.alloc(heap, KEY_NULL, 0));
        sl.setRoot(HeadIndex.alloc(heap, sl.getHead(), 0, 0, 0));
        return sl;
    }
    
    public void setComparator(KeyComparator comp) {
        this.comparator = comp;
    }
    
    long getRoot() {
        return Unsafe.getLongVolatile(this.addr + OFFSET_ROOT);
    }
    
    /**
     * 
     * @param pKey
     * @return a pointer to a 32 bits variable
     */
    public long put(long pKey) {
        if (pKey == 0) {
            throw new IllegalArgumentException();
        }
        return doPut(pKey);
    }

    private long doPut(long pKey) {
        long z = 0;
        outer: for (;;) {
            for (long b = findPredecessor(pKey), n = Node.getNext(b);;) {
                if (n != 0) {
                    int c;
                    long f = Node.getNext(n);
                    if (n != Node.getNext(b))               // inconsistent read
                        break;
                    if ((c = compare(pKey, Node.getKeyPointer(n))) > 0) {
                        b = n;
                        n = f;
                        continue;
                    }
                    if (c == 0) {
                        return Node.getValuePointer(n);
                    }
                    // else c < 0; fall through
                }

                z = Node.alloc(this.heap, pKey, n);
                if (!Node.casNext(b, n, z))
                    break;         // restart if lost race to append to b
                break outer;
            }
        }
    
        try {
            buildIndex(pKey, z);
        }
        catch (OutOfHeapException x) {
            // failed to add index node doesnt hurt
        }
        
        return z;
    }
    
    /**
     * Returns a base-level node with key strictly less than given key,
     * or the base-level header if there is no such node.  Also
     * unlinks indexes to deleted nodes found along the way.  Callers
     * rely on this side-effect of clearing indices to deleted nodes.
     */
    private long findPredecessor(long pKey) {
        for (;;) {
            for (long q = getRoot(), r = IndexNode.getRight(getRoot()),d;;) {
                if (r != 0 && !isDeleted(r)) {
                    long oNode = IndexNode.getNode(r);
                    if (compare(pKey, Node.getKeyPointer(oNode)) > 0) {
                        q = r;
                        r = IndexNode.getRight(r);
                        continue;
                    }
                }
                if ((d = IndexNode.getDown(q)) == 0) {
                    return IndexNode.getNode(q);
                }
                q = d;
                r = IndexNode.getRight(d);
            }
        }
    }
    
    private boolean isDeleted(long r) {
        return false;
    }

    final int compare(long pKeyX, long pKeyY) {
        return this.comparator.compare(pKeyX, pKeyY);
    }

    private void buildIndex(long pKey, long z) {
        int rnd = ThreadLocalRandom.current().nextInt();
        if ((rnd & 0x80000001) != 0) { 
            // test highest and lowest bits
            return;
        }
        int level = 1, max;
        while (((rnd >>>= 1) & 1) != 0)
            ++level;
        long idx = 0;
        long h = getRoot();
        if (level <= (max = HeadIndex.getLevel(getRoot()))) {
            for (int i = 1; i <= level; ++i)
                idx = IndexNode.alloc(heap, z, idx, 0);
        }
        else { // try to grow by one level
            level = max + 1; // hold in array and later pick the one to use
            long[] idxs = new long[level+1];
            for (int i = 1; i <= level; ++i)
                idxs[i] = idx = IndexNode.alloc(heap, z, idx, 0);
            for (;;) {
                h = getRoot();
                int oldLevel = HeadIndex.getLevel(h);
                if (level <= oldLevel) // lost race to add level
                    break;
                long newh = h;
                long oldbase = IndexNode.getNode(h);
                for (int j = oldLevel+1; j <= level; ++j)
                    newh = HeadIndex.alloc(heap, oldbase, newh, idxs[j], j);
                if (casRoot(h, newh)) {
                    h = newh;
                    idx = idxs[level = oldLevel];
                    break;
                }
            }
        }
        // find insertion points and splice in
        splice: for (int insertionLevel = level;;) {
            int j = HeadIndex.getLevel(h);
            for (long q = h, r = IndexNode.getRight(q), t = idx;;) {
                if (q == 0 || t == 0)
                    break splice;
                if (r != 0 && !isDeleted(r)) {
                    long n = IndexNode.getNode(r);
                    // compare before deletion check avoids needing recheck
                    int c = compare(pKey, Node.getKeyPointer(n));
                    if (c > 0) {
                        q = r;
                        r = IndexNode.getRight(r);
                        continue;
                    }
                }

                if (j == insertionLevel) {
                    if (!IndexNode.link(q, r, t))
                        break; // restart
                    if (--insertionLevel == 0)
                        break splice;
                }

                if (--j >= insertionLevel && j < level)
                    t = IndexNode.getDown(t);
                q = IndexNode.getDown(q);
                r = IndexNode.getRight(q);
            }
        }
    }
    
    private boolean casRoot(long cmp, long val) {
        return Unsafe.compareAndSwapLong(this.addr + OFFSET_ROOT, cmp, val);
    }
    
    /**
     * find the value
     * 
     * @param pKey pointer to the key 
     * @return a pointer to a 32 bits value
     */
    public long get(long pKey) {
        if (pKey == 0) {
            throw new IllegalArgumentException();
        }
        long node = findNode(pKey, true, false, false);
        if (node == 0) {
            return 0;
        }
        if (Node.isDeleted(node)) {
            return 0;
        }
        return Node.getValuePointer(node);
    }
    
    long findNode(long pKey, boolean inclusive, boolean returnLeft, boolean returnRight) {
        if (inclusive) {
            if (returnLeft) {
                return findNode(pKey, OP_LE);
            }
            else if (returnRight) {
                return findNode(pKey, OP_GE);
            }
            else {
                return findNode(pKey, OP_EQUAL);
            }
        }
        else {
            if (returnLeft) {
                return findNode(pKey, OP_LT);
            }
            else if (returnRight) {
                return findNode(pKey, OP_GT);
            }
            else {
                throw new IllegalArgumentException();
            }
        }
    }

    long findNode(long pKey, int op) {
        for (;;) {
            long oNode = findNode_(pKey, op);
            if (oNode == 0) {
                return 0;
            }
            if (!Node.isDeleted(oNode)) { 
                return oNode;
            }
        }
    }
    
    long findNode_(long pKey, int op) {
        if (pKey == 0) {
            if ((op == OP_LE) || (op == OP_LT)) {
                // find tail
                return getTailNode();
            }
            else if ((op == OP_GE) || (op == OP_GT)) {
                return getHeadNode();
            }
            else {
                return 0;
            }
        }
        
        // drill down
        long node = 0;
        for (long q = getRoot(), r = IndexNode.getRight(getRoot()),d;;) {
            if (r != 0 && !isDeleted(r)) {
                long oNode = IndexNode.getNode(r);
                int cmp = compare(pKey, Node.getKeyPointer(oNode));
                if ((cmp == 0) && ((op == OP_GE) || (op == OP_LE) || (op == OP_EQUAL))) {
                    // found it
                    return oNode;
                }
                if (cmp > 0) {
                    // we are greater than the node on the right, go right
                    q = r;
                    r = IndexNode.getRight(r);
                    continue;
                }
            }
            // reached bottom break the loop
            if ((d = IndexNode.getDown(q)) == 0) {
                node = IndexNode.getNode(q);
                break;
            }
            // not at bottom, go down one level
            q = d;
            r = IndexNode.getRight(d);
        }
        
        // step through linked list at the bottom
        for (long i=node; i!=0;) {
            long next = Node.getNext(i);
            if (next == 0) {
                // at the end of list
                if ((op == OP_LE) || (op == OP_LT)) {
                    return (i == getHead()) ? 0 : i;
                }
                else {
                    return 0;
                }
            }
            int cmp = compare(pKey, Node.getKeyPointer(next));
            if (cmp > 0) {
                // next
                i = next;
                continue;
            }
            if (cmp == 0) {
                if ((op == OP_GE) || (op == OP_LE) || (op == OP_EQUAL)) {
                    return next;
                }
                if (op == OP_LT) {
                    return i;
                }
                else {
                    return Node.getNext(next);
                }
            }
            else {
                // cmp < 0
                if ((op == OP_LT) || (op == OP_LE)) {
                    return (i == getHead()) ? 0 : i;
                }
                if ((op == OP_GT) || (op == OP_GE)) {
                    return next;
                }
                else {
                    return 0;
                }
            }
        }
        return 0;
    }

    public long getHead() {
        return Unsafe.getLongVolatile(this.addr + OFFSET_HEAD);
    }
    
    private long getHeadNode() {
        long oNext = Node.getNext(getHead());
        return oNext;
    }
    
    private long getTailNode() {
        for (long i=getRoot();;) {
            long oNext = IndexNode.getRight(i);
            if (oNext != 0) {
                i = oNext;
                continue;
            }
            long oDown = IndexNode.getDown(i);
            if (oDown != 0) {
                i = oDown;
                continue;
            }
            long oNode = IndexNode.getNode(i);
            for (long j=oNode;;) {
                long oNextNode = Node.getNext(j);
                if (oNextNode == 0) {
                    return j;
                }
                j = oNextNode;
            }
        }
    }
    
    private void setHead(long value) {
        Unsafe.putLongVolatile(this.addr + OFFSET_HEAD, value);
    }

    private void setRoot(long value) {
        Unsafe.putLongVolatile(this.addr + OFFSET_ROOT, value);
    }

    public int getLevel() {
        return HeadIndex.getLevel(getRoot());
    }

    public long getAddress() {
        return this.addr;
    }
    
}
