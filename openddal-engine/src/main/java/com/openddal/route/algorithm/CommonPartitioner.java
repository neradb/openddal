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
package com.openddal.route.algorithm;

import java.util.List;
import java.util.Set;

import com.openddal.route.rule.ObjectNode;
import com.openddal.route.rule.RuleEvaluateException;
import com.openddal.util.New;
import com.openddal.util.StringUtils;
import com.openddal.value.CompareMode;
import com.openddal.value.Value;
import com.openddal.value.ValueLong;
import com.openddal.value.ValueNull;

/**
 * @author jorgie.li
 */
public abstract class CommonPartitioner implements Partitioner {

    private int defaultNodeIndex = -1;

    private ObjectNode[] tableNodes;

    protected static int[] toIntArray(String string) {
        String[] split = StringUtils.arraySplit(string, ',', true);
        int[] ints = new int[split.length];
        for (int i = 0; i < split.length; ++i) {
            ints[i] = Integer.parseInt(split[i]);
        }
        return ints;
    }

    /**
     * @return the defaultNodeIndex
     */
    public int getDefaultNodeIndex() {
        return defaultNodeIndex;
    }

    /**
     * @param defaultNodeIndex the defaultNodeIndex to set
     */
    public void setDefaultNodeIndex(int defaultNodeIndex) {
        this.defaultNodeIndex = defaultNodeIndex;
    }

    /**
     * @return the tableNodes
     */
    public ObjectNode[] getTableNodes() {
        return tableNodes;
    }

    public void initialize(ObjectNode[] tableNodes) {
        this.tableNodes = tableNodes;
    }

    @Override
    public Integer[] partition(Value beginValue, Value endValue) {
        List<Value> values = enumRange(beginValue, endValue);
        if (values == null) {
            return allNodes();
        }
        return partition(values.toArray(new Value[values.size()]));
    }

    @Override
    public Integer[] partition(Value... values) {
        Set<Integer> result = New.linkedHashSet();
        for (Value value : values) {
            Integer partition = partition(value);
            result.add(partition);
        }
        return result.toArray(new Integer[result.size()]);
    }

    protected Integer[] allNodes() {
        Integer[] result = new Integer[tableNodes.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = i;
        }
        return result;
    }

    protected List<Value> enumRange(Value firstV, Value lastV) {
        if (isNull(firstV) || isNull(lastV)) {
            return null;
        }
        if (firstV.getType() != lastV.getType()) {
            return null;
        }
        int type = firstV.getType();
        switch (type) {
            case Value.BYTE:
            case Value.INT:
            case Value.LONG:
            case Value.SHORT:
                if (lastV.subtract(firstV).getLong() > 200) {
                    return null;
                }
                List<Value> enumValues = New.arrayList(10);
                Value enumValue = firstV;
                CompareMode compareMode = CompareMode.getInstance(null, 0);
                while (enumValue.compareTo(lastV, compareMode) <= 0) {
                    enumValues.add(enumValue);
                    Value increase = ValueLong.get(1).convertTo(enumValue.getType());
                    enumValue = enumValue.add(increase);
                }
                return enumValues;

            default:
                return null;
        }

    }

    /**
     * @param value
     */
    protected boolean checkNull(Value value) {
        if (isNull(value)) {
            if (getDefaultNodeIndex() == -1) {
                throw new RuleEvaluateException(
                        "No default node defined when no rule column value from SQL");
            }
            return true;
        } else {
            return false;
        }
    }
    
    protected static boolean isNull(Value v) {
        return v == null || v == ValueNull.INSTANCE;
    }

}
