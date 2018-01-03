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
package com.antsdb.saltedfish.sql;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.sql.Types;

import org.apache.commons.lang.NotImplementedException;

import com.antsdb.saltedfish.cpp.*;
import com.antsdb.saltedfish.lexer.MysqlParser.Data_typeContext;

/**
 * Created by wguo on 15-01-02.
 */
public class DataType {
    int length = -1;
    int scale = -1;
    int sqlType;
    Class<?> klass;
    byte fishType;
    String name;
    
    public DataType(String name, int length, int scale, int sqlType, Class<?> klass, byte fishType) {
        this.length = length;
        this.scale = scale;
        this.name = name;
        this.sqlType = sqlType;
        this.klass = klass;
        this.fishType = fishType;
    }
    
    @Override
	public boolean equals(Object obj) {
        	if (!(obj instanceof DataType)) {
        		return false;
        	}
        	DataType that =(DataType)obj;
        	if (this.fishType != that.fishType) {
        		return false;
        	}
        	if (this.length != that.length) {
        		return false;
        	}
        	if (this.scale != that.scale) {
        		return false;
        	}
        	return true;
	}

    public int getLength() {
        return length;
    }

	public void setLength(int value) {
    	    this.length = value;
    }
    
    public int getScale() {
        return scale;
    }

    public int getSqlType() {
    	    return this.sqlType;
    }

    public Class<?> getJavaType() {
        return this.klass;
    }

    static DataType _varchar = new TypeString("_varchar", 4000, Types.VARCHAR);
    public static DataType varchar() {
        return _varchar;
    }

    public static DataType varchar(int length) {
    	    return new TypeString("_varchar", length, Types.VARCHAR);
	}

    static DataType _number = new TypeDecimal("_decimal", 38, 10);
    public static DataType number() {
        return _number;
    }
    
    static DataType _float = new TypeFloat("_float");
    public static DataType floatType() {
        return _float;
    }
    
    static DataType _double = new TypeFloat("_double");
    public static DataType doubleType() {
        return _double;
    }
    
    static DataType _integer = new TypeInteger(
    		"_integer", 
    		Types.INTEGER, 
    		Integer.class, 
    		Value.TYPE_NUMBER, 
    		Long.MIN_VALUE, 
    		Long.MAX_VALUE);
    public static DataType integer() {
        return _integer;
    }

    static DataType _long = new TypeInteger(
    		"_long", 
    		Types.BIGINT, 
    		Long.class, 
    		Value.TYPE_NUMBER, 
    		Long.MIN_VALUE, 
    		Long.MAX_VALUE);
    public static DataType longtype() {
        return _long;
    }

    static DataType _boolean = new TypeBoolean("_boolean");
    public static DataType bool() {
        return _boolean;
    }

    static DataType _date = new TypeDate("_date");
	public static DataType date() {
		return _date;
	}
	
    static DataType _time = new TypeTime("_time");
	public static DataType time() {
		return _time;
	}

    static DataType _timestamp = new TypeTimestamp("_timestamp", 0);
    public static DataType timestamp() {
        return _timestamp;
    }

    static DataType _clob = new TypeClob("_clob", Types.CLOB, Long.MAX_VALUE);
    public static DataType clob() { 
        return _clob; 
	}

    static DataType _blob = new TypeBlob("_blob", Types.BLOB, Long.MAX_VALUE);
    public static DataType blob() {
        return _blob;
    }
    
    public static DataType parse(DataTypeFactory fac, Data_typeContext rule) {
        String typeName = null;
        int length = 0;
        int scale = 0;
        if (rule.data_type_nothing() != null) {
            typeName = rule.data_type_nothing().any_name().getText();
        }
        else if (rule.data_type_length() != null) {
            typeName = rule.data_type_length().any_name().getText();
            length = Integer.parseInt(rule.data_type_length().signed_number().getText());
        }
        else if (rule.data_type_length_scale() != null) {
            typeName = rule.data_type_length_scale().any_name().getText();
            length = Integer.parseInt(rule.data_type_length_scale().signed_number(0).getText());
            scale = Integer.parseInt(rule.data_type_length_scale().signed_number(1).getText());
        }
        return fac.newDataType(typeName, length, scale);
    }
    
    @Override
    public String toString() {
    	    String text;
    	    if (this.getJavaType() == String.class) {
    	        text = this.name + '(' + length + ')';
    	    }
    	    else if (this.getJavaType() == byte[].class) {
            text = this.name + '(' + length + ')';
        }
    	    else if (this.getJavaType() == BigDecimal.class) {
    	        text = this.name + '(' + length + ',' + scale + ')';
    	    } 
    	    else {
    	        text = this.name;
    	    }
        return text;
    }

    public boolean validate(Object val) {
        if (val == null) {
            return true;
        }
        if (getJavaType() == String.class) {
            if (val instanceof String) {
                return true;
            }
            if (!(val instanceof String)) {
            }
        }
        else if (getJavaType() == byte[].class) {
            if (val instanceof byte[]) {
                return true;
            }
        }
        else if (getJavaType() == Long.class) {
            if (val instanceof Long) {
                return true;
            }
        }
        else if (getJavaType() == Integer.class) {
            if (val instanceof Integer) {
                return true;
            }
        }
        else if (getJavaType() == Float.class) {
            if (val instanceof Float) {
                return true;
            }
        }
        else if (getJavaType() == Double.class) {
            if (val instanceof Double) {
                return true;
            }
        }
        else if (getJavaType() == BigDecimal.class) {
            if (val instanceof BigDecimal) {
                return true;
            }
        }
        else if (getJavaType() == Boolean.class) {
            if (val instanceof Boolean) {
                return true;
            }
        }
        else if (getJavaType() == Timestamp.class) {
            if (val instanceof Timestamp) {
                return true;
            }
        }
        else {
            throw new NotImplementedException();
        }
        return false;
    }

    public byte getFishType() {
    	return fishType;
    }

	public String getName() {
		return this.name;
	}

}
