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
package com.antsdb.saltedfish.server.mysql.util;

/**
 * @author roger
 */
public class BindValue {

    public boolean isNull; /* NULL indicator */
    public boolean isLongData; /* long data indicator */
    public boolean isSet; /* has this parameter been set */

    public long length; /* Default length of data */
    public int type; /* data type */
    public byte scale;

    public byte byteBinding;
    public short shortBinding;
    public int intBinding;
    public float floatBinding;
    public long longBinding;
    public double doubleBinding;
    public Object value; /* Other value to store */

    public void reset() {
        this.isNull = false;
        this.isLongData = false;
        this.isSet = false;

        this.length = 0;
        this.type = 0;
        this.scale = 0;

        this.byteBinding = 0;
        this.shortBinding = 0;
        this.intBinding = 0;
        this.floatBinding = 0;
        this.longBinding = 0L;
        this.doubleBinding = 0D;
        this.value = null;
    }

    public Object getValue()
    {
    	Object val = null;
        if (!this.isNull) 
        {
            if (this.isSet)
            {
                switch (this.type) {
                case Fields.FIELD_TYPE_TINY:
                	val = Integer.valueOf(this.byteBinding);
                    break;
                case Fields.FIELD_TYPE_SHORT:
                	val = Integer.valueOf(this.shortBinding);
                    break;
                case Fields.FIELD_TYPE_LONG:
                	val = Integer.valueOf(this.intBinding);
                    break;
                case Fields.FIELD_TYPE_LONGLONG:
                	val = Long.valueOf(this.longBinding);
                    break;
                case Fields.FIELD_TYPE_FLOAT:
                	val = Float.valueOf(this.floatBinding);
                    break;
                case Fields.FIELD_TYPE_DOUBLE:
                	val = Double.valueOf(this.doubleBinding);
                    break;
                case Fields.FIELD_TYPE_TIME:
                case Fields.FIELD_TYPE_DATE:
                case Fields.FIELD_TYPE_DATETIME:
                case Fields.FIELD_TYPE_TIMESTAMP:
                case Fields.FIELD_TYPE_VAR_STRING:
                case Fields.FIELD_TYPE_STRING:
                case Fields.FIELD_TYPE_VARCHAR:
                case Fields.FIELD_TYPE_DECIMAL:
                case Fields.FIELD_TYPE_BLOB:
                case Fields.FIELD_TYPE_NEW_DECIMAL:
                	val = this.value;
                    break;
                default:
                    throw new IllegalArgumentException("bindValue error,unsupported type:" + this.type);
                }
            }
        }
        else
        {
            value = null;
        }
        return val;

    }
    public String toString()
    {
    	return "type:"+type+";isnull:"+isNull+";value:"+getValue();
    }
}