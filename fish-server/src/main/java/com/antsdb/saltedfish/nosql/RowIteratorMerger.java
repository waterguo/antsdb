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

import com.antsdb.saltedfish.cpp.KeyComparator;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * merges two RowIterator. the upper one has can overwrite lower one
 * 
 * @author wgu0
 */
class RowIteratorMerger implements RowIterator {
	RowIterator upper, lower, lastFetched;
	KeyComparator comparator;
	
	RowIteratorMerger(RowIterator upper, RowIterator lower, KeyComparator comparator) {
		this.upper = upper;
		this.lower = lower;
		this.comparator = comparator;
	}
	
	@Override
	public boolean next() {
		if (this.lastFetched == null) {
			this.upper.next();
			this.lower.next();
			this.lastFetched = this.upper;
		}
		else {
			this.lastFetched.next();
		}
		
		// if eof on one of the pipe, switch to the other

		long pKey = this.lastFetched.getRowKeyPointer();
		RowIterator other = (this.lastFetched == this.upper) ? this.lower : this.upper;
		if (pKey == 0) {
			this.lastFetched = other;
			return this.lastFetched.next();
		}
		
		// if only one of the iterator is eof. simple case
		
		long pKeyOther = other.getRowKeyPointer();
		if (pKeyOther == 0) {
			return true;
		}
		
		// both have values
		
		int cmp = compare(pKey, pKeyOther);
		if (cmp < 0) {
		}
		else if (cmp > 0) {
			this.lastFetched = other;
		}
		else {
			this.lastFetched = this.upper;
			lower.next();
		}
		return true;
	}

	@Override
	public long getRowKeyPointer() {
		if (this.lastFetched != null) {
			return this.lastFetched.getRowKeyPointer();
		}
		else {
			return 0;
		}
	}

	@Override
	public long getRowPointer() {
		if (this.lastFetched != null) {
			return this.lastFetched.getRowPointer();
		}
		else {
			return 0;
		}
	}

	@Override
	public long getVersion() {
		if (this.lastFetched != null) {
			return this.lastFetched.getVersion();
		}
		else {
			return 0;
		}
	}

	@Override
	public long getRowScanned() {
		return this.upper.getRowScanned() + this.lower.getRowScanned();
	}

	@Override
	public void rewind() {
		throw new NotImplementedException();
	}

	@Override
	public Row getRow() {
		if (this.lastFetched != null) {
			return this.lastFetched.getRow();
		}
		else {
			return null;
		}
	}

	private int compare(long pKeyX, long pKeyY) {
		int result = this.comparator.compare(pKeyX, pKeyY);
		return result;
	}

	@Override
	public void close() {
		this.upper.close();
		this.lower.close();
	}

	@Override
	public long getKeyPointer() {
		if (this.lastFetched != null) {
			return this.lastFetched.getKeyPointer();
		}
		else {
			return 0;
		}
	}

	@Override
	public boolean isRow() {
		return this.lastFetched.isRow();
	}

	@Override
	public long getIndexSuffix() {
		return this.lastFetched.getIndexSuffix();
	}

	@Override
	public boolean eof() {
		return this.lower.eof() && this.upper.eof();
	}

	@Override
	public byte getMisc() {
		return this.lastFetched.getMisc();
	}

}
