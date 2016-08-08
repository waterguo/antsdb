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
package com.antsdb.saltedfish.cpp;

/**
 * 
 * @author wgu0
 */
public class SkipListScanner {
	FishSkipList list;
	int end;
	int start;
	boolean ascending;
	int current = -1;

	public long getValuePointer() {
		long result = FishSkipList.Node.getValuePointer(list.base, this.current);
		return result;
	}

	public int getValueOffset() {
		int result = FishSkipList.Node.getValueOffset(this.current);
		return result;
	}

	public long getKeyPointer() {
		long result = list.base + getKeyOffset();
		return result;
	}
	
	public int getKeyOffset() {
		int result = FishSkipList.Node.getKeyOffset(current);
		return result;
	}
	
	public int getNodeOffset() {
		return current;
	}
	
	public boolean eof() {
		return this.current == 0;
	}
	
	public boolean next() {
		if (this.current == -1) {
			this.current = this.start;
		}
		else {
			fetch();
		}
		return this.current != 0;
	}
	
	void fetch() {
		if (this.current == this.end) {
			this.current = 0;
			return;
		}
		if (this.ascending) {
			fetchNext();
		}
		else {
			fetchPrevious();
		}
	}

	private void fetchPrevious() {
		long pKey = FishSkipList.Node.getKeyPointer(this.list.base, this.current);
		int offset = this.list.findNode(pKey, false, true, false);
		this.current = offset;
	}

	private void fetchNext() {
		this.current = FishSkipList.Node.getNext(this.list.base, this.current);
	}

	public void rewind() {
		this.current = -1;
	}

}
