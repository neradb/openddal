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
package com.openddal.route.rule;

import java.io.Serializable;
import java.util.List;

import com.openddal.util.New;
import com.openddal.value.Value;

/**
 * @author jorgie.li
 */
public class RoutingArgument implements Serializable {

    public static final int NONE_ROUTING_ARGUMENT = 0;
    public static final int FIXED_ROUTING_ARGUMENT = 1;
    public static final int RANGE_ROUTING_ARGUMENT = 2;
    private static final long serialVersionUID = 1L;
    
    private final String columnName;

    private int argumentType;

    private List<Value> values;

    private Value start;

    private Value end;

    public RoutingArgument(String columnName) {
        this.columnName = columnName;
        this.argumentType = NONE_ROUTING_ARGUMENT;
    }
    
    public RoutingArgument(String columnName, Value value) {
        this.columnName = columnName;
        this.argumentType = FIXED_ROUTING_ARGUMENT;
        this.values = New.arrayList(1);
        this.values.add(value);
    }

    public RoutingArgument(String columnName, List<Value> values) {
        this.columnName = columnName;
        this.argumentType = FIXED_ROUTING_ARGUMENT;
        this.values = values;
    }

    public RoutingArgument(String columnName, Value start, Value end) {
        this.columnName = columnName;
        this.argumentType = RANGE_ROUTING_ARGUMENT;
        this.start = start;
        this.end = end;
    }
    
    public String getColumnName() {
        return columnName;
    }

    public int getArgumentType() {
        return argumentType;
    }

    public List<Value> getValues() {
        return values;
    }

    public Value getStart() {
        return start;
    }

    public Value getEnd() {
        return end;
    }
}
