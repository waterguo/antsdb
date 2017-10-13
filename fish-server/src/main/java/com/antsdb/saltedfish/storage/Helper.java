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

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.commons.codec.Charsets;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.BluntHeap;
import com.antsdb.saltedfish.cpp.FastDecimal;
import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.FishUtf8;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.Int4Array;
import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.cpp.Unicode16;
import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.cpp.Value;
import com.antsdb.saltedfish.nosql.IndexLine;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.VaporizingRow;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.sql.vdm.KeyMaker;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * hbase helper functions but in a more user friendly way
 *  
 * @author wgu0
 */
final class Helper {
    final static Logger _log = UberUtil.getThisLogger();
    
	private static final byte[] EMPTY = new byte[0];

    public static final String SYS_COLUMN_FAMILY 	= "s";			// all system information columns use "s" as column family
    public static final String DATA_COLUMN_FAMILY 	= "d";         	// all data columns use same column family - "d"

    public static final String SYS_COLUMN_KEYPREFIX = "k";      // key id (first 4 bytes) of "Key" in antsdb
    public static final String SYS_COLUMN_DATATYPE 	= "t";			// array of bytes to store data type for each column
    public static final String SYS_COLUMN_VERSION 	= "v";			// version of row in antsdb
    public static final String SYS_COLUMN_INDEXKEY 	= "i";         	// index key column name
    public static final String SYS_COLUMN_MISC      = "m";          // misc
    public static final String SYS_COLUMN_SIZE      = "s";         // size in bytes
    public static final String SYS_COLUMN_HASH      = "h";
    
    public static final byte[] SYS_COLUMN_FAMILY_BYTES = Bytes.toBytes(SYS_COLUMN_FAMILY);
    public static final byte[] DATA_COLUMN_FAMILY_BYTES = Bytes.toBytes(DATA_COLUMN_FAMILY);    
    public static final byte[] SYS_COLUMN_KEYPREFIX_BYTES = Bytes.toBytes(SYS_COLUMN_KEYPREFIX);
    public static final byte[] SYS_COLUMN_DATATYPE_BYTES = Bytes.toBytes(SYS_COLUMN_DATATYPE);    
    public static final byte[] SYS_COLUMN_VERSION_BYTES = Bytes.toBytes(SYS_COLUMN_VERSION);
    public static final byte[] SYS_COLUMN_INDEXKEY_BYTES = Bytes.toBytes(SYS_COLUMN_INDEXKEY);    
    public static final byte[] SYS_COLUMN_MISC_BYTES = Bytes.toBytes(SYS_COLUMN_MISC);    
    public static final byte[] SYS_COLUMN_SIZE_BYTES = Bytes.toBytes(SYS_COLUMN_SIZE);
    public static final byte[] SYS_COLUMN_HASH_BYTES = Bytes.toBytes(SYS_COLUMN_HASH);

    public static Map<String, byte[]> toMap(Result r) {
		Map<String, byte[]> row = new HashMap<>();
		byte[] key = r.getRow();
		row.put("", key);
		for (Map.Entry<byte[],NavigableMap<byte[],byte[]>> i:r.getNoVersionMap().entrySet()) {
			String cf =  new String(i.getKey());
			for (Map.Entry<byte[],byte[]> j:i.getValue().entrySet()) {
				String q = new String(getKeyName(j.getKey()));
				String name = cf + ":" + q;
				row.put(name, j.getValue());
			}
		}
		return row;
	}

    public static Put toPut(Mapping mapping, Row row) {
        byte[] key = Helper.antsKeyToHBase(row.getKeyAddress());
        Put put = new Put(key);
        put.addColumn(SYS_COLUMN_FAMILY_BYTES, SYS_COLUMN_SIZE_BYTES, Bytes.toBytes(row.getLength()));
        put.addColumn(SYS_COLUMN_FAMILY_BYTES, SYS_COLUMN_HASH_BYTES, Bytes.toBytes(row.getHash()));
        
        // populate fields
        
        int maxColumnId = row.getMaxColumnId();
        byte[] types = new byte[maxColumnId+1];
        for (int i=0; i<=maxColumnId; i++) {
            long pValue = row.getFieldAddress(i); 
            types[i] = Helper.getType(pValue);
            byte[] value = Helper.toBytes(pValue);
            put.addColumn(mapping.getUserFamily(), mapping.getColumn(i), value);
        }

        // populate data types
        put.addColumn(Helper.SYS_COLUMN_FAMILY_BYTES, Helper.SYS_COLUMN_DATATYPE_BYTES, types);
        return put;
    }
    
    public static Put toPut(IndexLine line) {
        byte[] key = Helper.antsKeyToHBase(line.getKey());
        byte[] rowKey = Helper.antsKeyToHBase(line.getRowKey());
        byte[] misc = new byte[1];
        misc[0] = line.getMisc();
        Put put = new Put(key);
        put.addColumn(Helper.SYS_COLUMN_FAMILY_BYTES, Helper.SYS_COLUMN_INDEXKEY_BYTES, rowKey);
        put.addColumn(Helper.SYS_COLUMN_FAMILY_BYTES, Helper.SYS_COLUMN_MISC_BYTES, misc);
        return put;
    }

	public static String getKeyName(byte[] key) {
		if ((key.length == 2) && (key[0] < 0x30)) {
			int column = key[0] << 8 | key[1];
			return Integer.toString(column);
		}
		return Bytes.toString(key);
	}
	
    public static Object hBaseDataToObject(int valueType, byte[] value) {

    	if (valueType == Value.TYPE_NULL) {
    		return null;
    	}
    	
    	if (value == null || 
    			(value.length == 0 && (valueType != Value.FORMAT_UNICODE16 && valueType != Value.FORMAT_UTF8)))
    	{
    		return null;
    	}
    	
    	Object result = null;
    	if (valueType == Value.FORMAT_INT4) {
    		result = Bytes.toInt(value);
    	}
    	else if (valueType == Value.FORMAT_INT8) {
    		result = Bytes.toLong(value);
		}
		else if (valueType == Value.FORMAT_NULL) {
			result = null;
		}
		else if (valueType == Value.FORMAT_BOOL) {
			result = Bytes.toBoolean(value);
		}
		else if (valueType == Value.FORMAT_DECIMAL) {
			result = Bytes.toBigDecimal(value);
		}
		else if (valueType == Value.FORMAT_FAST_DECIMAL) {
			result = Bytes.toBigDecimal(value);
//			FastDecimal fastDecimal = (FastDecimal)FishObject.get(null, valueAddr);
			// convert fastDecimal to bytes...
			//valueByte = ByteBuffer.wrap(Bytes.toBytes(fastDecimal));
		}
		else if (valueType == Value.FORMAT_FLOAT4) {
			result = Bytes.toFloat(value);
		}
		else if (valueType == Value.FORMAT_FLOAT8) {
			result = Bytes.toDouble(value);
		}
		else if (valueType == Value.FORMAT_UNICODE16) {
			result = value;
			char[] chars = new char[value.length / 2];
			for (int i=0; i<value.length / 2; i++) {
				chars[i] = (char)((((int)value[i*2+1]) << 8) | value[i*2]);
			}
			result = new String(chars);
		}
		else if (valueType == Value.FORMAT_DATE) {
			long time = Bytes.toLong(value);
			result = new Date(time);
		}
		else if (valueType == Value.FORMAT_TIMESTAMP) {
			long time = Bytes.toLong(value);
			result = new Timestamp(time);
		}
		else if (valueType == Value.FORMAT_BYTES) {
			result = value;
		}
		else {
			throw new NotImplementedException();
		}
    	return result;
    }  
    
    public static boolean existsNamespace(Connection conn, String nameSpace) {
        try (Admin admin = conn.getAdmin()) {
            NamespaceDescriptor ns = admin.getNamespaceDescriptor(nameSpace);            
            return (ns != null);
        } catch (NamespaceNotFoundException ex) {
            return false;
        } catch (Exception ex) {
            throw new OrcaHBaseException(ex);
        }
    }

    public static boolean existsTable(Connection conn, TableName name) {
        try (Admin admin = conn.getAdmin()) {
            return admin.isTableAvailable(name);            
        } 
        catch (TableNotFoundException ex) {
            return false;
        } 
        catch (Exception ex) {
            throw new OrcaHBaseException(ex, "Failed to check existence of table {}.{}", name);
        }
    }
    
	public static boolean existsTable(Connection conn, String ns, String name) {
    	return existsTable(conn, TableName.valueOf(ns, name));            
	}

    public static void createNamespace(Connection connection, String namespace) {        
        // Check whether namespace exists
        if (!Helper.existsNamespace(connection, namespace)) {
            try (Admin admin = connection.getAdmin()) {
                NamespaceDescriptor nsDescriptor = NamespaceDescriptor.create(namespace).build();
                _log.debug("creating namespace {}", namespace);
                admin.createNamespace(nsDescriptor);
            } 
            catch(Exception ex) {
                throw new OrcaHBaseException("Failed to create namespace - " + namespace, ex);
            }
        }
        else {
            //throw new HumpbackException("Namespace already exists - " + namespace);
        }
    }

    public static void dropNamespace(Connection connection, String namespace) {
        // Check whether namespace exists
        if (Helper.existsNamespace(connection, namespace)) {
	        try (Admin admin = connection.getAdmin()) { 
                _log.debug("dropping namespace {}", namespace);
	            admin.deleteNamespace(namespace);
	        } 
	        catch(Exception ex) {
	            throw new OrcaHBaseException("Failed to drop namespace - " + namespace, ex);
	        }
        } 
        else {
            // throw new HumpbackException("Namespace not found - " + namespace);
        }
    }
    
    public static void createTable(Connection connection, String namespace, String tableName) {
    	
		// Check whether table already exists
    	if (!Helper.existsTable(connection, namespace, tableName)) {
    		
        	// Create namespace first
        	createNamespace(connection, namespace);
        
        	// Create table
        	try (Admin admin = connection.getAdmin()) {
                HTableDescriptor table = new HTableDescriptor(TableName.valueOf(namespace, tableName));
                table.addFamily(new HColumnDescriptor(SYS_COLUMN_FAMILY));
                table.addFamily(new HColumnDescriptor(DATA_COLUMN_FAMILY));
                _log.debug("creating table {}", table.toString());
                admin.createTable(table);
			} catch (Exception ex) {
				throw new OrcaHBaseException("Failed to create table - " + tableName, ex);
			}
    	}
    }

    public static void createTable(Connection conn, String namespace, String tableName, Algorithm compressionType) {
		// Check whether table already exists
        if (Helper.existsTable(conn, namespace, tableName)) {
            Helper.dropTable(conn, namespace, tableName);
        }
    	if (!Helper.existsTable(conn, namespace, tableName)) {
    		
        	// Create namespace first
        	createNamespace(conn, namespace);
        
        	// Create table
        	try (Admin admin = conn.getAdmin()) {
                HTableDescriptor table = new HTableDescriptor(TableName.valueOf(namespace, tableName));
                table.addFamily(new HColumnDescriptor(SYS_COLUMN_FAMILY).setCompressionType(compressionType));
                table.addFamily(new HColumnDescriptor(DATA_COLUMN_FAMILY).setCompressionType(compressionType));
                _log.debug("creating table {}", table.toString());
                admin.createTable(table);
			} 
        	catch (Exception ex) {
				throw new OrcaHBaseException(ex, "Failed to create table - " + tableName);
			}
    	}
    }
    
    public static void dropTable(Connection connection, String namespace, String tableName) {
    	try (Admin admin = connection.getAdmin()) {

    		// Check whether table already exists
        	TableName table = TableName.valueOf(namespace, tableName);
    		if (admin.tableExists(table)) {
	            // Drop table
    		    _log.debug("dropping table {}", table.toString());
	            admin.disableTable(table);
	            admin.deleteTable(table);
    		}
        } 
    	catch (Exception ex) {
            throw new OrcaHBaseException("Failed to drop table - " + tableName, ex);
        }
    }
    
    public static void truncateTable(Connection connection, String namespace, String tableName) {

    	try {
    	
        	TableName table = TableName.valueOf(namespace, tableName);
        	
    		// get compression type
    		Table htable = connection.getTable(table);  		
	        HTableDescriptor tableDesc = htable.getTableDescriptor();
	        HColumnDescriptor[] families = tableDesc.getColumnFamilies();
	        Algorithm compressionType =  families[0].getCompression();
	        
	        // drop table
	        dropTable(connection, namespace, tableName);
	        
	        // create table
	    	createTable(connection, namespace, tableName, compressionType);

    	} catch (Exception ex) {
    		throw new OrcaHBaseException("Failed to truncate table - " + tableName, ex);
    	}
    }
    
    public static void setTruncateTableSP(Connection connection, int tableid, long sp) {
    	
    }

    public static Result exist(Connection conn, TableName tableName, byte[] key) throws IOException {
		Table htable = conn.getTable(tableName);
		Get get = new Get(key);
		get.addColumn(SYS_COLUMN_FAMILY_BYTES, SYS_COLUMN_VERSION_BYTES);
		Result r = htable.get(get);
		return r;
	}
	
	public static Result get(Connection conn, TableName tableName, byte[] key) throws IOException {
		Table htable = conn.getTable(tableName);
		Get get = new Get(key);
		Result r = htable.get(get);
		return r;
	}

	public static void getRowKey(BluntHeap heap, TableMeta table, Result r) {
		byte[] key = hbaseKeyToAnts(r.getRow());
		KeyBytes.allocSet(heap, key);
	}

	public static long getVersion(Result r) {
	    NavigableMap<byte[], byte[]> sys = r.getFamilyMap(SYS_COLUMN_FAMILY_BYTES);
	    byte[] versionBytes = sys.get(SYS_COLUMN_VERSION_BYTES);
	    long version = Bytes.toLong(versionBytes);
		return version;
	}

	public static long toRow(Heap heap, Result r, TableMeta table, int tableId) {
		if (r.isEmpty()) {
			return 0;
		}

		// some preparation
		
	    NavigableMap<byte[], byte[]> sysFamilyMap = r.getFamilyMap(SYS_COLUMN_FAMILY_BYTES);
	    NavigableMap<byte[], byte[]> dataFamilyMap = r.getFamilyMap(DATA_COLUMN_FAMILY_BYTES);
	    byte[] colDataType = sysFamilyMap.get(SYS_COLUMN_DATATYPE_BYTES);
	    byte[] sizeBytes = sysFamilyMap.get(SYS_COLUMN_SIZE_BYTES);
	    int size = Bytes.toInt(sizeBytes);
	    
	    // populate the row. system table doesn't come with metadata
	    
		VaporizingRow row = null;
	    if (table != null) {
	    	row = populateUsingMetadata(heap, table, dataFamilyMap, colDataType, size);
	    }
	    else {
	    	row = populateDirect(heap, dataFamilyMap, colDataType, size);
	    }
		byte[] key = hbaseKeyToAnts(r.getRow());
		row.setKey(key);
    	long pRow = Row.from(heap, row);
    	return pRow;
	}

	public static byte[] hbaseKeyToAnts(byte[] bytes) {
		KeyMaker.flipEndian(bytes);
		return bytes;
	}

	private static VaporizingRow populateDirect(
			Heap heap, 
			NavigableMap<byte[], 
			byte[]> data, 
			byte[] types,
			int size) {
	    VaporizingRow row = new VaporizingRow(heap, types.length-1);
		for (Map.Entry<byte[], byte[]> i:data.entrySet()) {
			byte[] key = i.getKey();
			int column = key[0] << 8 | key[1];
			if (types[column] == Value.FORMAT_NULL) {
				continue;
			}
			long pValue = toMemory(heap, types[column], i.getValue());
        	row.setFieldAddress(column, pValue);
		}
		return row;
	}

	private static VaporizingRow populateUsingMetadata(
			Heap heap, 
			TableMeta table, 
			NavigableMap<byte[], byte[]> data, 
			byte[] types,
			int size) {
	    VaporizingRow row = new VaporizingRow(heap, table.getMaxColumnId());
        for (int i=0; i<=table.getMaxColumnId(); i++) {
            if (i == 0) {
                // rowid
                byte[] colRowid = Bytes.toBytes((short)0);
                byte[] colValueBytes = data.get(colRowid);
                long pValue = toMemory(heap, Value.FORMAT_INT8, colValueBytes);
                row.setFieldAddress(i, pValue);
                continue;
            }
            ColumnMeta colMeta;
        	colMeta = table.getColumnByColumnId(i);
            // skip invalid column - not found column meta
            if (colMeta == null) {
            	continue;
            }
            // skip non-existing column
            if (i >= types.length) {
            	break;
            }
            // Add column
            // Get column name
            String colName = colMeta.getColumnName();
            byte[] colNameBytes = Bytes.toBytes(colName);
            byte[] colValueBytes = data.get(colNameBytes);
            long pValue = toMemory(heap, types[i], colValueBytes);
            row.setFieldAddress(i, pValue);
        }
        return row;
	}

	public static byte getType(long pValue) {
		if (pValue == 0) {
			return Value.TYPE_NULL;
		}
		return Value.getFormat(null, pValue);
	}

	private static long putBytesReversed(Heap heap, int type, byte[] value) {
        long p = heap.alloc(value.length + 1);
        Unsafe.putByte(p, (byte)type);
	    for (int i=0; i<value.length; i++) {
	        Unsafe.putByte(p+1+i, value[value.length-i-1]);
	    }
	    return p;
	}
	
    public static long toMemory(Heap heap, int type, byte[] value) {
        if (value == null) {
            return 0;
        }
        long pValue;
        if (type == Value.FORMAT_INT4) {
            pValue = putBytesReversed(heap, type, value);
        }
        else if (type == Value.FORMAT_INT8) {
            pValue = putBytesReversed(heap, type, value);
        }
        else if (type == Value.FORMAT_NULL) {
            return 0;
        }
        else if (type == Value.FORMAT_BOOL) {
            pValue = putBytesReversed(heap, type, value);
        }
        else if (type == Value.FORMAT_FLOAT4) {
            pValue = putBytesReversed(heap, type, value);
        }
        else if (type == Value.FORMAT_FLOAT8) {
            pValue = putBytesReversed(heap, type, value);
        }
        else if (type == Value.FORMAT_DATE) {
            pValue = putBytesReversed(heap, type, value);
        }
        else if (type == Value.FORMAT_TIMESTAMP) {
            pValue = putBytesReversed(heap, type, value);
        }
        else if (type == Value.FORMAT_TIME) {
            pValue = putBytesReversed(heap, type, value);
        }
        else if (type == Value.FORMAT_DECIMAL) {
            BigDecimal bd = Bytes.toBigDecimal(value);
            return FishObject.allocSet(heap, bd);
        }
        else if (type == Value.FORMAT_FAST_DECIMAL) {
            BigDecimal bd = Bytes.toBigDecimal(value);
            pValue = FastDecimal.allocSet(heap, bd);
        }
        else if (type == Value.FORMAT_UTF8) {
            pValue = heap.alloc(value.length + FishUtf8.HEADER_SIZE);
            Unsafe.putByte(pValue, (byte)type);
            Unsafe.putInt3(pValue+1, value.length);
            Unsafe.putBytes(pValue+FishUtf8.HEADER_SIZE, value);
        }
        else if (type == Value.FORMAT_UNICODE16) {
            String s = Bytes.toString(value);
            pValue = Unicode16.allocSet(heap, s);
        }
        else if (type == Value.FORMAT_BYTES) {
            pValue = com.antsdb.saltedfish.cpp.Bytes.allocSet(heap, value);
        }
        else if (type == Value.FORMAT_INT4_ARRAY) {
            pValue = Int4Array.alloc(heap, value).getAddress();
        }
        else {
            throw new IllegalArgumentException(String.valueOf(type));
        }
        return pValue;
    }
    
	public static byte[] toBytes(long pValue) {
		if (pValue == 0) {
			return EMPTY;
		}
		
        int length = 0;
        int offset = 1;

        // get data type
        byte dataType = Unsafe.getByte(pValue);
        boolean needInverse = true;
		if (dataType == Value.FORMAT_INT4) {
			length = 4;
		}
		else if (dataType == Value.FORMAT_INT8) {
			length = 8;
		}
		else if (dataType == Value.FORMAT_NULL) {
			length = 0;
		}
		else if (dataType == Value.FORMAT_BOOL) {
			length = 1;
		}
		else if (dataType == Value.FORMAT_FLOAT4) {
			length = 4;
		}
		else if (dataType == Value.FORMAT_FLOAT8) {
			length = 8;
		}
		else if (dataType == Value.FORMAT_DATE) {
			length = 8;
		}
		else if (dataType == Value.FORMAT_TIMESTAMP) {
			length = 8;
		}
		else if (dataType == Value.FORMAT_TIME) {
			length = 8;
		}
		else if (dataType == Value.FORMAT_DECIMAL) {
			BigDecimal bigDecimal = (BigDecimal)FishObject.get(null, pValue);
			return Bytes.toBytes(bigDecimal);
		}
		else if (dataType == Value.FORMAT_FAST_DECIMAL) {
			BigDecimal fastDecimal = (BigDecimal)FishObject.get(null, pValue);
			return Bytes.toBytes(fastDecimal);
		}
		else if (dataType == Value.FORMAT_UTF8) {
			return FishUtf8.getBytes(pValue);
		}
		else if (dataType == Value.FORMAT_UNICODE16) {
			String s = Unicode16.get(null, pValue);
			return s.getBytes(Charsets.UTF_8);
		}
		else if (dataType == Value.FORMAT_BYTES) {
			needInverse = false;
			length = Unsafe.getInt3(pValue + 1);
			offset = 4;
		}
		else if (dataType == Value.FORMAT_INT4_ARRAY) {
		    return new Int4Array(pValue).toBytes();
		}
		else {
			throw new IllegalArgumentException(String.valueOf(dataType));
		}
        
		byte[] bytes = new byte[length];
		Unsafe.getBytes(pValue + offset, bytes);
		if (needInverse) {
			for (int i=0; i<bytes.length/2; i++) {
				byte bt = bytes[i];
				bytes[i] = bytes[bytes.length - i - 1];
				bytes[bytes.length - i - 1] = bt;
			}
		}
		return bytes;
	}

    public static byte[] antsKeyToHBase(long pkey) {
    	byte[] bytes = KeyBytes.create(pkey).get();
    	KeyMaker.flipEndian(bytes);
    	return bytes;
    }

}
