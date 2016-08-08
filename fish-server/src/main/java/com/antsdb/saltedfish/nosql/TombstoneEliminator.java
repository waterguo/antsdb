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

/**
 * get rid of tombstone
 * 
 * @author wgu0
 */
class TombstoneEliminator implements RowIterator {
	RowIterator upstream;
	
	TombstoneEliminator(RowIterator upstream) {
		this.upstream = upstream;
	}
	
	@Override
	public boolean next() {
		for (;;) {
			if (this.upstream.next() == false) {
				return false;
			}
			long pRow = this.upstream.getRowPointer();
			if (!Row.isTombStone(pRow)) {
				return true;
			}
		}
	}

	@Override
	public long getRowKeyPointer() {
		return this.upstream.getRowKeyPointer();
	}

	@Override
	public long getRowPointer() {
		return this.upstream.getRowPointer();
	}

	@Override
	public long getVersion() {
		return this.upstream.getVersion();
	}

	@Override
	public long getRowScanned() {
		return this.upstream.getRowScanned();
	}

	@Override
	public void rewind() {
		this.upstream.rewind();
	}

	@Override
	public Row getRow() {
		return this.upstream.getRow();
	}

	@Override
	public void close() {
		this.upstream.close();
	}

	@Override
	public long getKeyPointer() {
		return this.upstream.getKeyPointer();
	}

	@Override
	public boolean isRow() {
		return this.upstream.isRow();
	}

}
