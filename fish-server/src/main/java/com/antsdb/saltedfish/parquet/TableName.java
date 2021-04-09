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
package com.antsdb.saltedfish.parquet;

import com.antsdb.saltedfish.nosql.SysMetaRow;

/**
 * 
 * @author Frank Li<lizc@tg-hd.com>
 */
public class TableName {

    private final static String TABLEPATH_FORMAT = "%s/%s-%d/";
    private final static String TABLEPATH_NOID_FORMAT = "%s/%s/";

    private final static String TABLEPATH_SHORT_FORMAT = "%s-%d/";
    private final static String TABLEPATH_SHORT_NOID_FORMAT = "%s/";

    private final static String NAME_FORMAT = "%s-%s";
    
    private final static String TABLEALLNAME_FORMAT = "%s-%s-%d";

    private int tableId;
    private String databaseName;
    private String tableName;

    private TableName(String databaseName, String tableName, int tableId) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.tableId = tableId;
    }

    public static TableName valueOf(String databaseName, String tableName) {
        return new TableName(databaseName, tableName, -1);
    }

    public static TableName valueOf(String databaseName, String tableName, int tableId) {
        return new TableName(databaseName, tableName, tableId);
    }
    public static TableName valueOf(SysMetaRow tablemeta ) {
        return new TableName(tablemeta.getNamespace(), tablemeta.getTableName(), tablemeta.getTableId());
    }
    
    public String getDatabasePath() {
        return databaseName + "/";
    }

    public String getTablePath() {
        String tebleAllPath =null;
        if(tableId>=0) {
            tebleAllPath = String.format(TABLEPATH_FORMAT, 
                    databaseName, 
                    tableName,
                    tableId);
        }
        else if(ObsService.TABLE_SYNC_PARAM.equalsIgnoreCase(tableName)){
            return "";
        }
        else{
            tebleAllPath = String.format(TABLEPATH_NOID_FORMAT, 
                    databaseName, 
                    tableName);
        }
        return tebleAllPath;
    }

    /**
     * s3 table dir
     * @return
     */
    public String getShortTablePath() {
        String tebleAllPath =null;
        if(tableId>0) {
            tebleAllPath = String.format(TABLEPATH_SHORT_FORMAT, 
                    tableName,
                    tableId);
        }
        else {
            tebleAllPath = String.format(TABLEPATH_SHORT_NOID_FORMAT, tableName);
        }
        return tebleAllPath;
    }

    public int getTableId() {
        return tableId;
    }

    public String getDatabaseTableName() {
        String tebleAllPath = String.format(NAME_FORMAT, databaseName, tableName);
        return tebleAllPath;
    }
    
    public String getDatabaseTableNameAndId() {
        String tebleAllPath = String.format(TABLEALLNAME_FORMAT, 
                    databaseName, 
                    tableName,
                    tableId);
        return tebleAllPath;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getTableNameAndId() {
        return String.format(NAME_FORMAT, tableName, tableId + "");
    }

    public boolean isEmpty() {
        return tableName == null || tableName.length() == 0;
    }

    @Override
    public String toString() {
        return "databaseName:" + databaseName + "\ttableName:" + tableName + "\t tableId:" + tableId;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object)
            return true;
        if (object instanceof TableName) {
            TableName other = (TableName) object;
            return this.tableId == other.tableId 
                    && this.tableName.equals(other.tableName)
                    && this.databaseName.equals(other.databaseName);
        }
        else {
            return false;
        }
    }

    @Override
    public final int hashCode() {
        int hashcode = 17;
        hashcode = hashcode * 31 + (int) this.tableId;
        hashcode = hashcode * 31 + this.tableName.hashCode();
        hashcode = hashcode * 31 + this.databaseName.hashCode();
        return hashcode;
    }
    
    public String getDataFilePrefix() { 
        return String.format("%08x", tableId);
    }

    public String getIndexFilePrefix() { 
        return String.format("%s-%08x",getTableName(), tableId);
    }
}
