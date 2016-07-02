/*
 * Copyright 2014-2016 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the “License”);
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
package com.openddal.command.dml;

import java.util.ArrayList;

import com.openddal.util.New;

/**
 * The list of setting for a SET statement.
 */
public class SetTypes {

    /**
     * The type of a SET IGNORECASE statement.
     */
    public static final int IGNORECASE = 1;

    /**
     * The type of a SET MODE statement.
     */
    public static final int MODE = 2;

    /**
     * The type of a SET COLLATION statement.
     */
    public static final int COLLATION = 3;

    /**
     * The type of a SET MAX_MEMORY_ROWS statement.
     */
    public static final int MAX_MEMORY_ROWS = 4;

    /**
     * The type of a SET ALLOW_LITERALS statement.
     */
    public static final int ALLOW_LITERALS = 5;

    /**
     * The type of a SET SCHEMA statement.
     */
    public static final int SCHEMA = 6;

    /**
     * The type of a SET SCHEMA_SEARCH_PATH statement.
     */
    public static final int SCHEMA_SEARCH_PATH = 7;

    /**
     * The type of a SET MAX_OPERATION_MEMORY statement.
     */
    public static final int MAX_OPERATION_MEMORY = 8;

    /**
     * The type of a SET QUERY_TIMEOUT statement.
     */
    public static final int QUERY_TIMEOUT = 9;

    /**
     * The type of a SET BINARY_COLLATION statement.
     */
    public static final int BINARY_COLLATION = 10;

    /**
     * The type of a SET \@VARIABLE statement.
     */
    public static final int VARIABLE = 11;

    /**
     * The type of a SET QUERY_STATISTICS_ACTIVE statement.
     */
    public static final int QUERY_STATISTICS = 12;

    /**
     * The type of a SET QUERY_STATISTICS_MAX_ENTRIES statement.
     */
    public static final int QUERY_STATISTICS_MAX_ENTRIES = 13;

    private static final ArrayList<String> TYPES = New.arrayList(QUERY_STATISTICS_MAX_ENTRIES);

    static {
        ArrayList<String> list = TYPES;
        list.add(null);
        list.add(IGNORECASE, "IGNORECASE");
        list.add(MODE, "IGNORECASE");
        list.add(COLLATION, "COLLATION");
        list.add(MAX_MEMORY_ROWS, "MAX_MEMORY_ROWS");
        list.add(ALLOW_LITERALS, "ALLOW_LITERALS");
        list.add(SCHEMA, "SCHEMA");
        list.add(SCHEMA_SEARCH_PATH, "SCHEMA_SEARCH_PATH");
        list.add(MAX_OPERATION_MEMORY, "MAX_OPERATION_MEMORY");
        list.add(QUERY_TIMEOUT, "QUERY_TIMEOUT");
        list.add(BINARY_COLLATION, "BINARY_COLLATION");
        list.add(VARIABLE, "BINARY_COLLATION");
        list.add(QUERY_STATISTICS, "QUERY_STATISTICS");
        list.add(QUERY_STATISTICS_MAX_ENTRIES, "QUERY_STATISTICS_MAX_ENTRIES");

    }

    private SetTypes() {
        // utility class
    }

    /**
     * Get the set type number.
     *
     * @param name the set type name
     * @return the number
     */
    public static int getType(String name) {
        for (int i = 0; i < getTypes().size(); i++) {
            if (name.equals(getTypes().get(i))) {
                return i;
            }
        }
        return -1;
    }

    public static ArrayList<String> getTypes() {
        return TYPES;
    }

    /**
     * Get the set type name.
     *
     * @param type the type number
     * @return the name
     */
    public static String getTypeName(int type) {
        return getTypes().get(type);
    }

}
