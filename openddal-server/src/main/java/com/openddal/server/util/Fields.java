/*
 * Copyright 2014-2016 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an “AS IS” BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.openddal.server.util;

/**
 * 字段类型及标识定义
 * 
 * @author mycat
 */
public interface Fields {

    /** field data type */
    public static final int FIELD_TYPE_DECIMAL = 0;
    public static final int FIELD_TYPE_TINY = 1;
    public static final int FIELD_TYPE_SHORT = 2;
    public static final int FIELD_TYPE_LONG = 3;
    public static final int FIELD_TYPE_FLOAT = 4;
    public static final int FIELD_TYPE_DOUBLE = 5;
    public static final int FIELD_TYPE_NULL = 6;
    public static final int FIELD_TYPE_TIMESTAMP = 7;
    public static final int FIELD_TYPE_LONGLONG = 8;
    public static final int FIELD_TYPE_INT24 = 9;
    public static final int FIELD_TYPE_DATE = 10;
    public static final int FIELD_TYPE_TIME = 11;
    public static final int FIELD_TYPE_DATETIME = 12;
    public static final int FIELD_TYPE_YEAR = 13;
    public static final int FIELD_TYPE_NEWDATE = 14;
    public static final int FIELD_TYPE_VARCHAR = 15;
    public static final int FIELD_TYPE_BIT = 16;
    public static final int FIELD_TYPE_NEW_DECIMAL = 246;
    public static final int FIELD_TYPE_ENUM = 247;
    public static final int FIELD_TYPE_SET = 248;
    public static final int FIELD_TYPE_TINY_BLOB = 249;
    public static final int FIELD_TYPE_MEDIUM_BLOB = 250;
    public static final int FIELD_TYPE_LONG_BLOB = 251;
    public static final int FIELD_TYPE_BLOB = 252;
    public static final int FIELD_TYPE_VAR_STRING = 253;
    public static final int FIELD_TYPE_STRING = 254;
    public static final int FIELD_TYPE_GEOMETRY = 255;

    /** field flag */
    public static final int NOT_NULL_FLAG = 0x0001;
    public static final int PRI_KEY_FLAG = 0x0002;
    public static final int UNIQUE_KEY_FLAG = 0x0004;
    public static final int MULTIPLE_KEY_FLAG = 0x0008;
    public static final int BLOB_FLAG = 0x0010;
    public static final int UNSIGNED_FLAG = 0x0020;
    public static final int ZEROFILL_FLAG = 0x0040;
    public static final int BINARY_FLAG = 0x0080;
    public static final int ENUM_FLAG = 0x0100;
    public static final int AUTO_INCREMENT_FLAG = 0x0200;
    public static final int TIMESTAMP_FLAG = 0x0400;
    public static final int SET_FLAG = 0x0800;

}