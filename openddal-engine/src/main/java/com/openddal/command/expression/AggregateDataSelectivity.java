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
import com.openddal.util.IntIntHashMap;
import com.openddal.value.Value;
import com.openddal.value.ValueInt;

/**
 * Data stored while calculating a SELECTIVITY aggregate.
 */
class AggregateDataSelectivity extends AggregateData {
    private long count;
    private IntIntHashMap distinctHashes;
    private double m2;

    @Override
    void add(Database database, int dataType, boolean distinct, Value v) {
        count++;
        if (distinctHashes == null) {
            distinctHashes = new IntIntHashMap();
        }
        int size = distinctHashes.size();
        if (size > Constants.SELECTIVITY_DISTINCT_COUNT) {
            distinctHashes = new IntIntHashMap();
            m2 += size;
        }
        int hash = v.hashCode();
        // the value -1 is not supported
        distinctHashes.put(hash, 1);
    }

    @Override
    Value getValue(Database database, int dataType, boolean distinct) {
        if (distinct) {
            count = 0;
        }
        Value v = null;
        int s = 0;
        if (count == 0) {
            s = 0;
        } else {
            m2 += distinctHashes.size();
            m2 = 100 * m2 / count;
            s = (int) m2;
            s = s <= 0 ? 1 : s > 100 ? 100 : s;
        }
        v = ValueInt.get(s);
        return v.convertTo(dataType);
    }
}
