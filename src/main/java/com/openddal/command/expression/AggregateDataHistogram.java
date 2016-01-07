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
package com.openddal.command.expression;

import com.openddal.engine.Constants;
import com.openddal.engine.Database;
import com.openddal.util.ValueHashMap;
import com.openddal.value.CompareMode;
import com.openddal.value.Value;
import com.openddal.value.ValueArray;
import com.openddal.value.ValueLong;

import java.util.Arrays;
import java.util.Comparator;

/**
 * Data stored while calculating a HISTOGRAM aggregate.
 */
class AggregateDataHistogram extends AggregateData {
    private long count;
    private ValueHashMap<AggregateDataHistogram> distinctValues;

    @Override
    void add(Database database, int dataType, boolean distinct, Value v) {
        if (distinctValues == null) {
            distinctValues = ValueHashMap.newInstance();
        }
        AggregateDataHistogram a = distinctValues.get(v);
        if (a == null) {
            if (distinctValues.size() < Constants.SELECTIVITY_DISTINCT_COUNT) {
                a = new AggregateDataHistogram();
                distinctValues.put(v, a);
            }
        }
        if (a != null) {
            a.count++;
        }
    }

    @Override
    Value getValue(Database database, int dataType, boolean distinct) {
        if (distinct) {
            count = 0;
            groupDistinct(database, dataType);
        }
        ValueArray[] values = new ValueArray[distinctValues.size()];
        int i = 0;
        for (Value dv : distinctValues.keys()) {
            AggregateDataHistogram d = distinctValues.get(dv);
            values[i] = ValueArray.get(new Value[]{dv, ValueLong.get(d.count)});
            i++;
        }
        final CompareMode compareMode = database.getCompareMode();
        Arrays.sort(values, new Comparator<ValueArray>() {
            @Override
            public int compare(ValueArray v1, ValueArray v2) {
                Value a1 = v1.getList()[0];
                Value a2 = v2.getList()[0];
                return a1.compareTo(a2, compareMode);
            }
        });
        Value v = ValueArray.get(values);
        return v.convertTo(dataType);
    }

    private void groupDistinct(Database database, int dataType) {
        if (distinctValues == null) {
            return;
        }
        count = 0;
        for (Value v : distinctValues.keys()) {
            add(database, dataType, false, v);
        }
    }

}
