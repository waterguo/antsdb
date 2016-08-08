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

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang.NotImplementedException;

/**
 * 
 * @author wgu0
 */
public class ConcurrentLinkedList<T> implements Iterable<T>, Collection<T> {
	AtomicReference<Node<T>> head = new AtomicReference<ConcurrentLinkedList.Node<T>>();
	AtomicReference<Node<T>> tail = new AtomicReference<ConcurrentLinkedList.Node<T>>();
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

	public void addFirst(T object) {
		Node<T> baby = new Node<>(object);
		for (;;) {
			Node<T> node = head.get();
			baby.next = node;
			if (head.compareAndSet(node, baby)) {
				break;
			}
		}
		if (baby.next == null) {
			tail.compareAndSet(null, baby);
		}
		this.size++;
	}
	
	public void addLast(T object) {
		Node<T> baby = new Node<>(object);
		for (;;) {
			Node<T> node = tail.get();
			if (node != null) {
				node.next = baby;
			}
			if (tail.compareAndSet(node, baby)) {
				break;
			}
		}
		if (this.head.get() == null) {
			head.compareAndSet(null, baby);
		}
		this.size++;
	}

	@Override
	public Iterator<T> iterator() {
		return new MyIterator<>(this.head.get());
	}

	@Override
	public void clear() {
		while (this.size != 0) {
			this.head.set(null);
			this.tail.set(null);
			this.size = 0;
		}
	}

	@Override
	public int size() {
		return this.size;
	}

	@Override
	public boolean isEmpty() {
		return this.head.get() == null;
	}

	@Override
	public boolean contains(Object o) {
		for (Node<T> i=this.head.get(); i!= null; i=i.next) {
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
	public boolean remove(Object o) {
		throw new NotImplementedException();
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
		return this.head.get();
	}
	
	public T getFirst() {
		Node<T> node = this.head.get();
		return (node != null) ? node.data : null;
	}

	public T getLast() {
		Node<T> node = this.tail.get();
		return (node != null) ? node.data : null;
	}

	public Node<T> insert(Node<T> node, T tablet) {
		Node<T> baby = new Node<T>(tablet);
		baby.next = node.next;
		node.next = baby;
		return baby;
	}

	public void deleteNext(Node<T> node) {
		if (node.next == null) {
			return;
		}
		node.next = node.next.next;
	}
}
