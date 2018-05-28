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

import java.io.File;
import java.util.List;

import org.slf4j.Logger;

import com.antsdb.saltedfish.nosql.VaporizingRow;
import com.antsdb.saltedfish.util.CodingError;
import com.antsdb.saltedfish.util.ConsoleHelper;
import com.antsdb.saltedfish.util.UberUtil;

import io.netty.util.internal.ThreadLocalRandom;

/**
 * my own version of skip list. direct memory access. super evil 
 * 
 * @author wgu0
 */
public final class FishSkipList implements ConsoleHelper {
	static final long KEY_NULL = Unsafe.allocateMemory(8);
	static final int OFFSET_ROOT = 0;
	static final int OFFSET_HEAD = 4;
	static final int OP_EQUAL = 0;
	static final int OP_LT = 1;
	static final int OP_GT = 2;
	static final int OP_GE = 3;
	static final int OP_LE = 4;
	static final int DELETE_MASK = 1;

	static final Logger _log = UberUtil.getThisLogger();
    static final VariableLengthLongComparator _comp = new VariableLengthLongComparator();
	
	private KeyComparator comparator = _comp;
	long base;
	BluntHeap heap;
	private long addr;
	
	static {
		Unsafe.putLong(KEY_NULL, 0);
		Unsafe.putByte(KEY_NULL, Value.FORMAT_KEY_BYTES);
	}
	
	static class IndexNode {
		protected final static int OFFSET_RIGHT_OFFSET = 0;
		protected final static int OFFSET_DOWN_OFFSET = OFFSET_RIGHT_OFFSET + 4;
		protected final static int OFFSET_NODE_OFFSET = OFFSET_DOWN_OFFSET + 4;
		protected final static int OFFSET_END = OFFSET_NODE_OFFSET + 4;
		
		final static int alloc(BluntHeap heap) {
			return heap.allocOffset(OFFSET_END);
		}
		
		final static int alloc(BluntHeap heap, int node , int down, int right) {
			int result = alloc(heap);
            if ((result & DELETE_MASK) != 0) {
                throw new CodingError();
            }
			heap.putIntVolatile(result + OFFSET_RIGHT_OFFSET, right);
			heap.putIntVolatile(result + OFFSET_DOWN_OFFSET, down);
			heap.putIntVolatile(result + OFFSET_NODE_OFFSET, node);
			return result;
		}
		
		final static int getRight(long base, int oIndexNode) {
            oIndexNode = oIndexNode & ~DELETE_MASK;
			return Unsafe.getIntVolatile(base + oIndexNode + OFFSET_RIGHT_OFFSET);
		}
		
		final static void setRight(long base, int oIndexNode, int right) {
            oIndexNode = oIndexNode & ~DELETE_MASK;
			Unsafe.putIntVolatile(base + oIndexNode + OFFSET_RIGHT_OFFSET, right);
		}
		
		final static boolean casRight(long base, int oIndexNode, int oldRight, int newRight) {
            oIndexNode = oIndexNode & ~DELETE_MASK;
			return Unsafe.compareAndSwapInt(base + oIndexNode + OFFSET_RIGHT_OFFSET, oldRight, newRight);
		}
		
		final static int getDown(long base, int oIndexNode) {
		    oIndexNode = oIndexNode & ~DELETE_MASK;
			return Unsafe.getIntVolatile(base + oIndexNode + OFFSET_DOWN_OFFSET);
		}
		
		final static int getNode(long base, int oIndexNode) {
            oIndexNode = oIndexNode & ~DELETE_MASK;
			return Unsafe.getIntVolatile(base + oIndexNode + OFFSET_NODE_OFFSET);
		}

		final static boolean link(long base, int oIndexNode, int oOldRight, int oNewRight) {
            oIndexNode = oIndexNode & ~DELETE_MASK;
			setRight(base, oNewRight, oOldRight);
			return casRight(base, oIndexNode, oOldRight, oNewRight);
		}

		public static Object toString(long base, int offset) {
			StringBuilder buf = new StringBuilder();
			buf.append("IN-");
			buf.append(String.format("%04x", offset));
			buf.append(" [");
			buf.append("right:");
			buf.append(IndexNode.getRight(base, offset));
			buf.append(" down:");
			buf.append(IndexNode.getDown(base, offset));
			buf.append(" node:");
			buf.append(IndexNode.getNode(base, offset));
			buf.append(']');
			return buf.toString();
		}
	}
	
	final static class HeadIndex extends IndexNode {
		protected final static int OFFSET_LEVEL_OFFSET = OFFSET_END;
		
		final static int alloc(BluntHeap heap, int node, int down, int right, int level) {
			int result = heap.allocOffset(OFFSET_END + 4);
			heap.putIntVolatile(result + OFFSET_RIGHT_OFFSET, right);
			heap.putIntVolatile(result + OFFSET_DOWN_OFFSET, down);
			heap.putIntVolatile(result + OFFSET_NODE_OFFSET, node);
			heap.putIntVolatile(result + OFFSET_LEVEL_OFFSET, level);
			return result;
		}
		
		final static int getLevel(long base, int oIndexNode) {
			return Unsafe.getIntVolatile(base + oIndexNode + OFFSET_LEVEL_OFFSET);
		}

		public static Object toString(long base, int offset) {
			StringBuilder buf = new StringBuilder();
			buf.append("HI-");
			buf.append(String.format("%04x", offset));
			buf.append(" [");
			buf.append("level:");
			buf.append(getLevel(base, offset));
			buf.append(" right:");
			buf.append(IndexNode.getRight(base, offset));
			buf.append(" down:");
			buf.append(IndexNode.getDown(base, offset));
			buf.append(" node:");
			buf.append(IndexNode.getNode(base, offset));
			buf.append(']');
			return buf.toString();
		}
	}
	
	public final static class Entry {
	    long p;
	    
	    public Entry(long p) {
	        this.p = p;
	    }
	    
	    public long getKeyPointer() {
	        return Node.getKeyPointer(this.p, 0);
	    }
	    
	    public long getValuePointer() {
	        return Node.getValuePointer(this.p, 0);
	    }
	}
	
	public final static class Node {
		private final static int OFFSET_VALUE_OFFSET = 0;
		private final static int OFFSET_NEXT_OFFSET = OFFSET_VALUE_OFFSET + 4;
		private final static int OFFSET_KEY = OFFSET_NEXT_OFFSET + 4;

		final static int alloc(BluntHeap heap, long pKey, int oNext ) {
			int size = (pKey != 0) ? KeyBytes.getRawSize(pKey) : 0;
			int result = heap.allocOffset(OFFSET_KEY + size);
            if ((result & DELETE_MASK) != 0) {
                // make sure result is 4 bytes aligned
                throw new CodingError();
            }
			setValue(heap, result, 0);
			setNext(heap, result, oNext);
			Unsafe.copyMemory(pKey, heap.getAddress(result + OFFSET_KEY), size);
			return result;
		}
		
		public final static int getKeyOffset(int oNode) {
		    oNode = oNode & ~DELETE_MASK;
			return oNode + OFFSET_KEY;
		}
		
		public final static int getNext(long base, int oNode) {
            oNode = oNode & ~DELETE_MASK;
			int result = Unsafe.getIntVolatile(base + oNode + OFFSET_NEXT_OFFSET);
			return result;
		}

		public static void setNext(BluntHeap heap, int oNode, int oNext) {
            oNode = oNode & ~DELETE_MASK;
			heap.putIntVolatile(oNode + OFFSET_NEXT_OFFSET, oNext);
		}

		public final static boolean casValue(BluntHeap heap, int oNode, int oldValue, int newValue) {
            oNode = oNode & ~DELETE_MASK;
			return heap.compareAndSwapInt(oNode + OFFSET_VALUE_OFFSET, oldValue, newValue);
		}

		final static int getValue(long base, int oNode) {
            oNode = oNode & ~DELETE_MASK;
			return Unsafe.getIntVolatile(base + oNode + OFFSET_VALUE_OFFSET);
		}
		
		public final static void setValue(BluntHeap heap, int oNode, int pValue) {
            oNode = oNode & ~DELETE_MASK;
			heap.putIntVolatile(oNode + OFFSET_VALUE_OFFSET, pValue);
		}

		public final static boolean isDeleted(long base, int oNode) {
            oNode = oNode & ~DELETE_MASK;
            int oNext = getNext(base, oNode);
            return (oNext & DELETE_MASK) != 0;
		}
            
		public final static boolean casNext(BluntHeap heap, int oNode, int oldValue, int newValue) {
            oNode = oNode & ~DELETE_MASK;
			return heap.compareAndSwapInt(oNode + OFFSET_NEXT_OFFSET, oldValue, newValue);
		}

		public final static long getKeyPointer(long base, int oNode) {
            oNode = oNode & ~DELETE_MASK;
			return base + oNode + OFFSET_KEY;
		}

		public final static int getValueOffset(int oNode) {
            oNode = oNode & ~DELETE_MASK;
			return oNode + OFFSET_VALUE_OFFSET;
		}
		
		public final static long getValuePointer(long base, int oNode) {
            oNode = oNode & ~DELETE_MASK;
			long p = base + oNode + OFFSET_VALUE_OFFSET;
			return p;
		}

		public static Object toString(long base, int offset) {
			StringBuilder buf = new StringBuilder();
			buf.append("ND-");
			buf.append(String.format("%04x", offset));
			buf.append(" [");
			buf.append("value:");
			buf.append(getValue(base, offset));
			buf.append(" next:");
			buf.append(getNext(base, offset));
			buf.append(" key:");
			buf.append(KeyBytes.toString(getKeyPointer(base, offset)));
			buf.append(']');
			return buf.toString();
		}
	}
	
	public FishSkipList(long base, int offset, KeyComparator comp) {
		this.base = base;
		this.addr = base + offset;
		if (comp != null) {
			this.comparator = comp;
		}
	}

	private FishSkipList() {
	}
	
	public static FishSkipList alloc(BluntHeap heap, KeyComparator comp) {
		FishSkipList slist = new FishSkipList();
		slist.base = heap.getAddress(0);
		slist.heap = heap;
		if (comp != null) {
	        slist.comparator = comp;
		}
		slist.addr = heap.alloc(8);
        slist.setHead(Node.alloc(heap, KEY_NULL, 0));
        slist.setRoot(HeadIndex.alloc(heap, slist.getHead(), 0, 0, 0));
		return slist;
	}
	
	public long getAddress() {
	    return this.addr;
	}
	
	int getRoot() {
		return Unsafe.getIntVolatile(this.addr + OFFSET_ROOT);
	}
	
	private void setRoot(int value) {
		Unsafe.putIntVolatile(this.addr + OFFSET_ROOT, value);
	}
	
	private boolean casRoot(int cmp, int val) {
		return Unsafe.compareAndSwapInt(this.addr + OFFSET_ROOT, cmp, val);
	}
    
	public int getHead() {
		return Unsafe.getIntVolatile(this.addr + OFFSET_HEAD);
	}
	
    private int getHeadNode() {
        int oNext = Node.getNext(this.base, getHead());
        return oNext;
    }
    
	private void setHead(int value) {
		Unsafe.putIntVolatile(this.addr + OFFSET_HEAD, value);
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
		int node = findNode(pKey, true, false, false);
		if (node == 0) {
			return 0;
		}
		if (Node.isDeleted(this.base, node)) {
		    return 0;
		}
		return Node.getValuePointer(this.base, node);
	}
	
    public boolean delete(long pKey) {
        // delete the node at bottom
        
        for (;;) {
            if (pKey == 0) {
                // invalid input
                return false;
            }
            int prev = findNode(pKey, OP_LT);
            if (prev == 0) {
                // not found
                return false;
            }
            int node = Node.getNext(this.base, prev);
            if (node <= DELETE_MASK) {
                // not found
                return false;
            }
            if (compare(pKey, Node.getKeyPointer(this.base, node)) != 0) {
                // not found
                return false;
            }
            if ((node & DELETE_MASK) != 0) {
                // prev is deleted, retry
                continue;
            }
            
            // set delete mark
            
            int next = Node.getNext(this.base, node);
            if ((next & DELETE_MASK) != 0) {
                // already deleted 
                return false;
            }
            if (!Node.casNext(this.heap, node, next, next | DELETE_MASK)) {
                continue;
            }
            
            // remove the node from linked list
            
            if (!Node.casNext(heap, prev, node, next)) {
                // cas failed, retry
                continue;
            }
            break;
        }
        
        // delete the index nodes
        
        retry: for (;;) {
            for (int q = getRoot(), r = IndexNode.getRight(this.base, getRoot()),d;;) {
                if (r > DELETE_MASK) {
                    int oNode = IndexNode.getNode(this.base, r);
                    int cmp = compare(pKey, Node.getKeyPointer(this.base, oNode));
                    if (cmp > 0) {
                        // we are greater than the node on the right, go right
                        q = r;
                        r = IndexNode.getRight(this.base, r);
                        continue;
                    }
                    if (cmp == 0) {
                        // found it, delete the index node, then go down one level
                        if ((r & DELETE_MASK) != 0) {
                            // retry. it is already deleted
                        }
                        int rr = IndexNode.getRight(this.base, r);
                        if (!IndexNode.casRight(this.base, r, rr, rr | DELETE_MASK)) {
                            // cas failed, retry
                            break;
                        }
                        if (!IndexNode.casRight(this.base, q, r, rr)) {
                            // cas failed, retry
                            break;
                        }
                    }
                }
                // reached bottom break the loop
                if ((d = IndexNode.getDown(this.base, q)) == 0) {
                    break retry;
                }
                // not at bottom, go down one level
                q = d;
                r = IndexNode.getRight(this.base, d);
            }
        }
        
        return true;
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
    	int z = 0;
        outer: for (;;) {
            for (int b = findPredecessor(pKey), n = Node.getNext(this.base, b);;) {
                if (n != 0) {
                    int c;
                    int f = Node.getNext(this.base, n);
                    if (n != Node.getNext(this.base, b))               // inconsistent read
                        break;
                    if ((c = compare(pKey, Node.getKeyPointer(this.base, n))) > 0) {
                        b = n;
                        n = f;
                        continue;
                    }
                    if (c == 0) {
                        if ((Node.getNext(this.base, n) & DELETE_MASK) != 0) {
                            // retry. this node is deleted
                            break;
                        }
                        return Node.getValuePointer(this.base, n);
                    }
                    // else c < 0; fall through
                }

                z = Node.alloc(this.heap, pKey, n);
                if (!Node.casNext(this.heap, b, n, z))
                    break;         // restart if lost race to append to b
                break outer;
            }
        }
    
    	try {
    	    buildIndex(pKey, z);
    	}
    	catch (OutOfHeapMemory x) {
    	    // failed to add index node doesnt hurt
    	}
	    
	    return Node.getValuePointer(this.base, z);
    }
    
    private void buildIndex(long pKey, int z) {
        int rnd = ThreadLocalRandom.current().nextInt();
        if ((rnd & 0x80000001) != 0) { 
            // test highest and lowest bits
            return;
        }
        int level = 1, max;
        while (((rnd >>>= 1) & 1) != 0)
            ++level;
        int idx = 0;
        int h = getRoot();
        if (level <= (max = HeadIndex.getLevel(this.base, getRoot()))) {
            for (int i = 1; i <= level; ++i)
                idx = IndexNode.alloc(heap, z, idx, 0);
        }
        else { // try to grow by one level
            level = max + 1; // hold in array and later pick the one to use
            int[] idxs = new int[level+1];
            for (int i = 1; i <= level; ++i)
                idxs[i] = idx = IndexNode.alloc(heap, z, idx, 0);
            for (;;) {
                h = getRoot();
                int oldLevel = HeadIndex.getLevel(this.base, h);
                if (level <= oldLevel) // lost race to add level
                    break;
                int newh = h;
                int oldbase = IndexNode.getNode(this.base, h);
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
            int j = HeadIndex.getLevel(this.base, h);
            for (int q = h, r = IndexNode.getRight(this.base, q), t = idx;;) {
                if (q == 0 || t == 0)
                    break splice;
                if (r > DELETE_MASK) {
                    int n = IndexNode.getNode(this.base, r);
                    // compare before deletion check avoids needing recheck
                    int c = compare(pKey, Node.getKeyPointer(this.base, n));
                    if (c > 0) {
                        q = r;
                        r = IndexNode.getRight(this.base, r);
                        continue;
                    }
                }

                if (j == insertionLevel) {
                    if (!IndexNode.link(this.base, q, r, t))
                        break; // restart
                    if (--insertionLevel == 0)
                        break splice;
                }

                if (--j >= insertionLevel && j < level)
                    t = IndexNode.getDown(this.base, t);
                q = IndexNode.getDown(this.base, q);
                r = IndexNode.getRight(this.base, q);
            }
        }
    }
    /**
     * Returns a base-level node with key strictly less than given key,
     * or the base-level header if there is no such node.  Also
     * unlinks indexes to deleted nodes found along the way.  Callers
     * rely on this side-effect of clearing indices to deleted nodes.
     */
    private int findPredecessor(long pKey) {
        for (;;) {
        	for (int q = getRoot(), r = IndexNode.getRight(this.base, getRoot()),d;;) {
        		if (r > DELETE_MASK) {
        			int oNode = IndexNode.getNode(this.base, r);
        			if (compare(pKey, Node.getKeyPointer(this.base, oNode)) > 0) {
        				q = r;
        				r = IndexNode.getRight(this.base, r);
        				continue;
        			}
        		}
        		if ((d = IndexNode.getDown(this.base, q)) == 0) {
        			return IndexNode.getNode(this.base, q);
        		}
        		q = d;
        		r = IndexNode.getRight(this.base, d);
        	}
        }
    }

	private final int compare(long pKeyX, long pKeyY) {
		return this.comparator.compare(pKeyX, pKeyY);
	}

    int findNode(long pKey, boolean inclusive, boolean returnLeft, boolean returnRight) {
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

    int findNode(long pKey, int op) {
        for (;;) {
            int oNode = findNode_(pKey, op);
            if (oNode == 0) {
                return 0;
            }
            if (!Node.isDeleted(this.base, oNode)) { 
                return oNode;
            }
        }
    }
    
    int findNode_(long pKey, int op) {
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
        
        int node = 0;
        for (int q = getRoot(), r = IndexNode.getRight(this.base, getRoot()),d;;) {
            if (r > DELETE_MASK) {
                int oNode = IndexNode.getNode(this.base, r);
                int cmp = compare(pKey, Node.getKeyPointer(this.base, oNode));
                if ((cmp == 0) && ((op == OP_GE) || (op == OP_LE) || (op == OP_EQUAL))) {
                    // found it
                    return oNode;
                }
                if (cmp > 0) {
                    // we are greater than the node on the right, go right
                    q = r;
                    r = IndexNode.getRight(this.base, r);
                    continue;
                }
            }
            // reached bottom break the loop
            if ((d = IndexNode.getDown(this.base, q)) == 0) {
                node = IndexNode.getNode(this.base, q);
                break;
            }
            // not at bottom, go down one level
            q = d;
            r = IndexNode.getRight(this.base, d);
        }
        
        // step through linked list at the bottom
        
        for (int i=node; i!=0;) {
            int next = Node.getNext(this.base, i);
            if (next <= DELETE_MASK) {
                // at the end of list
                if ((op == OP_LE) || (op == OP_LT)) {
                    return (i == getHead()) ? 0 : i;
                }
                else {
                    return 0;
                }
            }
            int cmp = compare(pKey, Node.getKeyPointer(this.base, next));
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
                    return Node.getNext(this.base,next);
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

    private FileOffset traceIo(List<FileOffset> lines, File file, long fileAddr, int offset, String note) {
        long fileOffset = this.addr - fileAddr + offset;
        FileOffset result = new FileOffset(file, fileOffset, note);
        lines.add(result);
        return result;
    }
    
    int traceIo(long pKey, int op, File file, long fileAddr, List<FileOffset> lines) {
        if (pKey == 0) {
            if ((op == OP_LE) || (op == OP_LT)) {
                // find tail
                return getTailNode();
            }
            else if ((op == OP_GE) || (op == OP_GT)) {
                traceIo(lines, file, fileAddr, getHead(), "head");
                traceIo(lines, file, fileAddr, getHeadNode(), "node");
                return getHeadNode();
            }
            else {
                return 0;
            }
        }
        
        // drill down
        
        int node = 0;
        traceIo(lines, file, fileAddr, OFFSET_ROOT, "root");
        traceIo(lines, file, fileAddr, getRoot(), "index");
        for (int q = getRoot(), r = IndexNode.getRight(this.base, getRoot()),d;;) {
            if (r > DELETE_MASK) {
                traceIo(lines, file, fileAddr, r, "index");
                int oNode = IndexNode.getNode(this.base, r);
                traceIo(lines, file, fileAddr, oNode, "node");
                int cmp = compare(pKey, Node.getKeyPointer(this.base, oNode));
                if ((cmp == 0) && ((op == OP_GE) || (op == OP_LE) || (op == OP_EQUAL))) {
                    // found it
                    return oNode;
                }
                if (cmp > 0) {
                    // we are greater than the node on the right, go right
                    q = r;
                    r = IndexNode.getRight(this.base, r);
                    continue;
                }
            }
            // reached bottom break the loop
            if ((d = IndexNode.getDown(this.base, q)) == 0) {
                node = IndexNode.getNode(this.base, q);
                break;
            }
            // not at bottom, go down one level
            q = d;
            r = IndexNode.getRight(this.base, d);
            traceIo(lines, file, fileAddr, q, "index");
        }
        
        // step through linked list at the bottom
        
        traceIo(lines, file, fileAddr, node, "node");
        for (int i=node; i!=0;) {
            int next = Node.getNext(this.base, i);
            if (next <= DELETE_MASK) {
                // at the end of list
                if ((op == OP_LE) || (op == OP_LT)) {
                    traceIo(lines, file, fileAddr, getHead(), "head");
                    return (i == getHead()) ? 0 : i;
                }
                else {
                    return 0;
                }
            }
            traceIo(lines, file, fileAddr, next, "node");
            int cmp = compare(pKey, Node.getKeyPointer(this.base, next));
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
                    return Node.getNext(this.base,next);
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

	@Override
	public String toString() {
		StringBuilder buf = new StringBuilder();
		for (int j=getRoot(); j!=0; j=IndexNode.getDown(this.base, j)) {
			buf.append(HeadIndex.toString(this.base, j));
			buf.append('\n');
			for (int i=IndexNode.getRight(this.base, j); i!=0; i=IndexNode.getRight(this.base, i)) {
				buf.append(IndexNode.toString(this.base, i));
				buf.append('\n');
			}
		}
		for (int i=getHead(); i!=0; i=Node.getNext(this.base, i)) {
			buf.append(Node.toString(this.base, i));
			buf.append('\n');
		}
		return buf.toString();
	}

	public SkipListScanner scan(
			long pKeyStart, 
			boolean includeStart, 
			long pKeyEnd, 
			boolean includeEnd) {
		SkipListScanner scanner = new SkipListScanner();
		scanner.ascending = true;
		scanner.list = this;
		scanner.start = findNode(pKeyStart, includeStart, false, true);
		if (scanner.start == 0) {
			return null;
		}
		scanner.end = findNode(pKeyEnd, includeEnd, true, false);
		long pKeyScanStart = Node.getKeyPointer(this.base, scanner.start);
		long pKeyScanEnd = Node.getKeyPointer(this.base, scanner.end);
		if (this.comparator.compare(pKeyScanStart, pKeyScanEnd) > 0) {
			// target range is not found
			return null;
		}
		return scanner;
	}

	public SkipListScanner scanReverse(
			long pKeyStart, 
			boolean includeStart, 
			long pKeyEnd, 
			boolean includeEnd) {
		SkipListScanner scanner = new SkipListScanner();
		scanner.ascending = false;
		scanner.list = this;
		scanner.start = findNode(pKeyStart, includeStart ? OP_LE : OP_LT);
		if (scanner.start == 0) {
			return null;
		}
		if (scanner.start == this.getHead()) {
		    return null;
		}
		scanner.end = findNode(pKeyEnd, includeEnd, false, true);
		if (scanner.end == 0) {
			return null;
		}
        long pKeyScanStart = Node.getKeyPointer(this.base, scanner.start);
        long pKeyScanEnd = Node.getKeyPointer(this.base, scanner.end);
        if (this.comparator.compare(pKeyScanStart, pKeyScanEnd) < 0) {
            // target range is not found
            return null;
        }
		return scanner;
	}
	
	public int size() {
		int count = 0;
		for (int i=getHead(); i!=0; i=Node.getNext(this.base, i)) {
			count++;
		}
		return count-1;
	}

	public boolean isEmpty() {
		return Node.getNext(this.base, getHead()) == 0;
	}
	
	long test;
	public void testEscape(VaporizingRow row) {
		this.test += row.getMaxColumnId();
	}

	public void dump() {
		for (int j=getRoot(); j!=0; j=IndexNode.getDown(this.base, j)) {
			println("%s", HeadIndex.toString(this.base, j));
			for (int i=IndexNode.getRight(this.base, j); i!=0; i=IndexNode.getRight(this.base, i)) {
				println("%s", IndexNode.toString(this.base, i));
			}
		}
		for (int i=getHead(); i!=0; i=Node.getNext(this.base, i)) {
			println("%s", Node.toString(this.base, i));
		}
	}
	
	public KeyComparator getComparator() {
		return this.comparator;
	}

	private int getTailNode() {
        for (int i=getRoot();;) {
            int oNext = IndexNode.getRight(this.base, i);
            if (oNext != 0) {
                i = oNext;
                continue;
            }
            int oDown = IndexNode.getDown(this.base, i);
            if (oDown != 0) {
                i = oDown;
                continue;
            }
            int oNode = IndexNode.getNode(this.base, i);
            for (int j=oNode;;) {
                int oNextNode = Node.getNext(this.base, j);
                if (oNextNode == 0) {
                    return j;
                }
                j = oNextNode;
            }
        }
	}
	
    public long getTail() {
        int oTail = getTailNode();
        if (oTail == 0) {
            return 0;
        }
        return Node.getValuePointer(this.base, oTail);
    }

    public long getTailKeyPointer() {
        int oTail = getTailNode();
        if (oTail == 0) {
            return 0;
        }
        return Node.getKeyPointer(this.base, oTail);
    }
    
    public long ceiling(long pKey) {
        if (pKey == 0) {
            return 0;
        }
        int node = findNode(pKey, OP_GE);
        if (node == 0) {
            return 0;
        }
        return Node.getValuePointer(this.base, node);
    }
    
    public long higher(long pKey) {
        if (pKey == 0) {
            return 0;
        }
        int node = findNode(pKey, OP_GT);
        if (node == 0) {
            return 0;
        }
        return Node.getValuePointer(this.base, node);
    }
    
    public long floor(long pKey) {
        if (pKey == 0) {
            return 0;
        }
        int node = findNode(pKey, OP_LE);
        if (node == 0) {
            return 0;
        }
        long result = Node.getValuePointer(this.base, node);
        if (result == this.base) {
            _log.debug("{} {}", this.base, node);
        }
        return result;
    }
    
    public long lower(long pKey) {
        if (pKey == 0) {
            return 0;
        }
        int node = findNode(pKey, OP_GT);
        if (node == 0) {
            return 0;
        }
        return Node.getValuePointer(this.base, node);
    }

    public long traceIo(long pKey, File file, long fileAddr, List<FileOffset> lines) {
        int node = this.traceIo(pKey, OP_EQUAL, file, fileAddr, lines);
        if (node == 0) {
            return 0;
        }
        if (Node.isDeleted(this.base, node)) {
            return 0;
        }
        return Node.getValuePointer(this.base, node);
    }
}
