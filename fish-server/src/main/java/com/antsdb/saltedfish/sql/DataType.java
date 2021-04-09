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
package com.antsdb.saltedfish.sql;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.function.Supplier;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;

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
    boolean zerofill = false;
    boolean isUnsigned = false;
    int weight;
    
    public DataType(String name, int length, int scale, int sqlType, Class<?> klass, byte fishType, int weight) {
        this.length = length;
        this.scale = scale;
        this.name = name;
        this.sqlType = sqlType;
        this.klass = klass;
        this.fishType = fishType;
        this.weight = weight;
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

    static DataType _varchar = new TypeString("varchar", Types.VARCHAR, 4000);
    public static DataType varchar() {
        return _varchar;
    }

    public static DataType varchar(int length) {
        return new TypeString("varchar", Types.VARCHAR, length);
    }

    static DataType _number = new TypeDecimal("decimal", 38, 4);
    public static DataType number() {
        return _number;
    }
    
    static DataType _float = new TypeFloat("float");
    public static DataType floatType() {
        return _float;
    }
    
    static DataType _double = new TypeDouble("double");
    public static DataType doubleType() {
        return _double;
    }
    
    static DataType _integer = new TypeInteger(
            "int", 
            Types.INTEGER, 
            Integer.class, 
            Value.TYPE_NUMBER, 
            0,
            Long.MIN_VALUE, 
            Long.MAX_VALUE);
    public static DataType integer() {
        return _integer;
    }

    static DataType _long = new TypeInteger(
            "bigint", 
            Types.BIGINT, 
            Long.class, 
            Value.TYPE_NUMBER, 
            0,
            Long.MIN_VALUE, 
            Long.MAX_VALUE);
    
    public static DataType longtype() {
        return _long;
    }

    static DataType _boolean = new TypeBoolean("_boolean");
    public static DataType bool() {
        return _boolean;
    }

    static DataType _date = new TypeDate("date");
    public static DataType date() {
        return _date;
    }
    
    static DataType _time = new TypeTime("time");
    public static DataType time() {
        return _time;
    }

    static DataType _timestamp = new TypeTimestamp("timestamp", 0);
    public static DataType timestamp() {
        return _timestamp;
    }

    static DataType _binary = new TypeBinary("blob", Types.VARBINARY, Long.MAX_VALUE);
    public static DataType binary() {
        return _binary;
    }
    
    static DataType _clob = new TypeClob("mediumtext", Types.CLOB, Long.MAX_VALUE);
    public static DataType clob() { 
        return _clob; 
    }

    static DataType _blob = new TypeBlob("mediumblob", Types.BLOB, Long.MAX_VALUE);
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
        if (this.getSqlType() == Types.CLOB) {
            text = this.name;
        }
        else if (this.getSqlType() == Types.BLOB) {
            text = this.name;
        }
        else if (this.getJavaType() == Timestamp.class) {
            if (length == 0) {
                text = this.name;
            }
            else {
                text = this.name + '(' + length + ')';
            }
        }
        else if (this.getJavaType() == String.class) {
            text = this.name + '(' + length + ')';
        }
        else if (this.getJavaType() == byte[].class) {
            text = this.name + '(' + length + ')';
        }
        else if (this.getJavaType() == BigDecimal.class) {
            text = this.name + '(' + length + ',' + scale + ')';
        } 
        else if (this.getJavaType() == Integer.class || this.getJavaType() == Long.class) {
            if (this.isUnsigned) {
                String name = StringUtils.removeEnd(this.name, " unsigned");
                text = name + '(' + length + ") unsigned";
            }
            else {
                text = this.name + '(' + length + ')';
            }
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

    public int getWeight() {
        return this.weight;
    }
    
    public String getName() {
        return this.name;
    }

    public boolean isZerofill() {
        return this.zerofill;
    }

    public void setZeroFill(boolean value) {
        this.zerofill = value;
    }

    public boolean isUnsigned() {
        return isUnsigned;
    }

    public void setUnsigned(boolean value) {
        this.isUnsigned = value;
    }
    
    /**
     * 
     * @param supplier
     * @return null if the supplier provides an empty set
     */
    public static DataType getUpCast(Supplier<DataType> supplier) {
        DataType result = null;
        for (;;) {
            DataType i = supplier.get();
            if (i == null) {
                break;
            }
            if (result == null) {
                result = i;
                continue;
            }
            result = getUpCast(result, i);
        }
        return result;
    }

    public static DataType getUpCast(DataType x, DataType y) {
        int familyx = x!=null ? Weight.getFamily(x.getWeight()) : 0;
        int familyy = y!=null ? Weight.getFamily(y.getWeight()) : 0;
        if (familyx == familyy) {
            return x.getWeight() >= y.getWeight() ? x : y;
        }
        if (familyx == Weight.FAMILY_BINARY || familyy == Weight.FAMILY_BINARY) {
            return DataType._binary;
        }
        if (familyx == Weight.FAMILY_CHAR || familyy == Weight.FAMILY_CHAR) {
            return DataType._varchar;
        }
        throw new IllegalArgumentException();
    }
}
