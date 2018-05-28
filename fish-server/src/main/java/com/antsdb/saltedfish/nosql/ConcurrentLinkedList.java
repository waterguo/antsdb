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
import java.util.Iterator;

import org.apache.commons.lang.NotImplementedException;

/**
 * 
 * @author wgu0
 */
public class ConcurrentLinkedList<T> implements Iterable<T>, Collection<T> {
	Node<T> head;
	volatile int size;
	
	private static class MyIterator<T> implements Iterator<T> {
		Node<T> node;
		
		MyIterator(Node<T> node) {
			this.node = node;
		}
		
		@Override
		public boolean hasNext() {
			return this.node != null;
		}

		@Override
		public T next() {
			T data = this.node.data;
			this.node = this.node.next;
			return data;
		}
	}
	
	public static class Node<T> {
		T data;
		volatile Node<T> next;
		
		Node(T object) {
			this.data = object;
		}
	}
	
	public ConcurrentLinkedList() {
	}
	
	public ConcurrentLinkedList(ConcurrentLinkedList<T> tablets) {
		for (T i:tablets) {
			addLast(i);
		}
	}

	public synchronized void addFirst(T object) {
		Node<T> baby = new Node<>(object);
        baby.next = head;
        head = baby;
		this.size++;
	}
	
	public synchronized void addLast(T object) {
        Node<T> baby = new Node<>(object);
        Node<T> last = getLastNode();
        if (last == null) {
            this.head = baby;
        }
        else {
            last.next = baby;
        }
		this.size++;
	}

	@Override
	public Iterator<T> iterator() {
		return new MyIterator<>(this.head);
	}

	@Override
	public synchronized void clear() {
		while (this.size != 0) {
			this.head = null;
			this.size = 0;
		}
	}

	@Override
	public int size() {
		return this.size;
	}

	@Override
	public boolean isEmpty() {
		return this.head == null;
	}

	@Override
	public boolean contains(Object o) {
		for (Node<T> i=this.head; i!= null; i=i.next) {
			if (i.data == o) {
				return true;
			}
		}
		return false;
	}

	@Override
	public Object[] toArray() {
		throw new NotImplementedException();
	}

	@SuppressWarnings("hiding")
	@Override
	public <T> T[] toArray(T[] a) {
		throw new NotImplementedException();
	}

	@Override
	public boolean add(T e) {
		throw new NotImplementedException();
	}

	@Override
	public synchronized boolean remove(Object o) {
	    if (this.head == null) {
	        return false;
	    }
	    if (this.head.data == o) {
	        this.head = this.head.next;
	        this.size--;
	        return true;
	    }
        for (Node<T> i=this.head; i!= null; i=i.next) {
            if (i.next == null) {
                continue;
            }
            if (i.next.data == o) {
                i.next = i.next.next;
                this.size--;
                return true;
            }
        }
        return false;
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		throw new NotImplementedException();
	}

	@Override
	public boolean addAll(Collection<? extends T> c) {
		throw new NotImplementedException();
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		throw new NotImplementedException();
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		throw new NotImplementedException();
	}

	public Node<T> getFirstNode() {
		return this.head;
	}
	
    public Node<T> getLastNode() {
        for (Node<T> i=head; i!=null; i=i.next) {
            if (i.next == null) {
                return i;
            }
        }
        return null;
    }
    
	public T getFirst() {
		Node<T> node = this.head;
		return (node != null) ? node.data : null;
	}

	public T getLast() {
	    Node<T> last = getLastNode();
        return (last != null) ? last.data : null;
	}

	public synchronized Node<T> insert(Node<T> node, T tablet) {
		Node<T> baby = new Node<T>(tablet);
		baby.next = node.next;
		node.next = baby;
		this.size++;
		return baby;
	}

	public synchronized void deleteNext(Node<T> node) {
		if (node.next == null) {
			return;
		}
		node.next = node.next.next;
		this.size--;
	}
}
