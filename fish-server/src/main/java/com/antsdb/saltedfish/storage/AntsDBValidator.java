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
package com.antsdb.saltedfish.storage;

import java.util.Arrays;

import javax.xml.bind.DatatypeConverter;

import org.apache.hadoop.hbase.TableName;

import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.RowIterator;
import com.antsdb.saltedfish.nosql.SysMetaRow;
import com.antsdb.saltedfish.nosql.TableType;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.OrcaConstant;
import com.antsdb.saltedfish.sql.meta.MetadataService;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.sql.vdm.Transaction;

public class AntsDBValidator {

	static TableName getTableName(Humpback humpback, int tableId) {
		if (tableId < 0) {
	    	String name = String.format("%08x", tableId);
			return TableName.valueOf(OrcaConstant.SYSNS, name);
		}
		SysMetaRow metarow = humpback.getTableInfo(tableId);
		return (metarow != null) ? TableName.valueOf(metarow.getNamespace(), metarow.getTableName()) : null;
	}

	static GTable getTable(Humpback humpback, String fullTableName) {
		GTable table = null;
		TableName tableName = TableName.valueOf(fullTableName);	
		String ns = tableName.getNamespaceAsString();
		String name = tableName.getNameAsString();
		for (GTable t : humpback.getTables(ns)) {
			TableName tn = getTableName(humpback, t.getId());
			if (name.equals(tn.getNameAsString())) {
				table = t;
				break;
			}
		}
		
		return table;
	}
	
	static TableMeta getTableMeta(MetadataService metaService, int tableId) {
		if (metaService == null) {
			return null;
		}
		if (tableId < 0) {
			return null;
		}
    	return metaService.getTable(Transaction.getSeeEverythingTrx(), tableId);
	}
	
	static byte[] hexToBytes(String hex) {
		
		if (hex == null || hex.isEmpty() || hex.length() % 2 == 1) {
			return null;
		}
		return DatatypeConverter.parseHexBinary(hex);
	}
	
	public static long checkTableRow(Orca orca, String fullTableName, String keyHex) {
		Humpback humpback = orca.getHumpback();
		
		GTable table = getTable(orca.getHumpback(), fullTableName);
		if (table == null) {
			System.out.println("Table not found - " + fullTableName);
			return -1;
		}
		SysMetaRow tableInfo = humpback.getTableInfo(table.getId());
		boolean isIndex = tableInfo.getType() == TableType.INDEX;
		
		byte[] key = hexToBytes(keyHex);
		if (key == null) {
			System.out.println("key must be a valid hex string like 0A12B3, even digits");
			return -1;
		}

		// Scan from head to get top #### rows
		RowIterator scanner;
		scanner = table.scan(0, Long.MAX_VALUE,
				0, 
                false, 
                0, 
                false, 
                true);
		long count = 0;
		while (scanner.next()) {
			// skip index table at this time
			if (isIndex) {
				System.out.println("Index table skipped - " + fullTableName);
				return -1;
			}
			
			Row row = scanner.getRow();
			byte[] antsKey = KeyBytes.create(row.getKeyAddress()).get();
			if (Arrays.equals(antsKey, key)) {
				count++;
				long version = row.getVersion();
				System.out.println(String.format("#%1$d - Version: %2$016x", count, version));
				System.out.println(row.toString());
			}
		}
		
		System.out.println("Total rows found: " + count);
		return count;
	}	
}
