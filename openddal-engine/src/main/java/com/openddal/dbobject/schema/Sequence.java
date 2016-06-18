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
package com.openddal.dbobject.schema;

import java.math.BigInteger;

import com.openddal.dbobject.DbObject;
import com.openddal.engine.Session;
import com.openddal.message.DbException;
import com.openddal.message.ErrorCode;

/**
 * A sequence is created using the statement CREATE SEQUENCE
 */
public class Sequence extends SchemaObject {

    public static final int DEFAULT_CACHE_SIZE = 100;

    private long value;
    private long increment;
    private long cacheSize;
    private long minValue;
    private long maxValue;
    private boolean cycle;
    private boolean belongsToTable;

    /**
     * Creates a new sequence for an auto-increment column.
     *
     * @param schema the schema
     * @param id the object id
     * @param name the sequence name
     * @param startValue the first value to return
     * @param increment the increment count
     */
    public Sequence(Schema schema, String name, long startValue, long increment) {
        this(schema, name, startValue, increment, null, null, null, false, false);
    }

    /**
     * Creates a new sequence.
     *
     * @param schema the schema
     * @param id the object id
     * @param name the sequence name
     * @param startValue the first value to return
     * @param increment the increment count
     * @param cacheSize the number of entries to pre-fetch
     * @param minValue the minimum value
     * @param maxValue the maximum value
     * @param cycle whether to jump back to the min value if needed
     * @param belongsToTable whether this sequence belongs to a table (for
     *            auto-increment columns)
     */
    public Sequence(Schema schema, String name, Long startValue,
            Long increment, Long cacheSize, Long minValue, Long maxValue,
            boolean cycle, boolean belongsToTable) {
        initSchemaObjectBase(schema, name);
        this.increment = increment != null ?
                increment : 1;
        this.minValue = minValue != null ?
                minValue : getDefaultMinValue(startValue, this.increment);
        this.maxValue = maxValue != null ?
                maxValue : getDefaultMaxValue(startValue, this.increment);
        this.value = startValue != null ?
                startValue : getDefaultStartValue(this.increment);
        this.cacheSize = cacheSize != null ?
                Math.max(1, cacheSize) : DEFAULT_CACHE_SIZE;
        this.cycle = cycle;
        this.belongsToTable = belongsToTable;
        if (!isValid(this.value, this.minValue, this.maxValue, this.increment)) {
            throw DbException.get(ErrorCode.SEQUENCE_ATTRIBUTES_INVALID, name,
                    String.valueOf(this.value), String.valueOf(this.minValue),
                    String.valueOf(this.maxValue),
                    String.valueOf(this.increment));
        }
    }

    /**
     * Validates the specified prospective start value, min value, max value and
     * increment relative to each other, since each of their respective
     * validities are contingent on the values of the other parameters.
     *
     * @param value the prospective start value
     * @param minValue the prospective min value
     * @param maxValue the prospective max value
     * @param increment the prospective increment
     */
    private static boolean isValid(long value, long minValue, long maxValue,
            long increment) {
        return minValue <= value &&
            maxValue >= value &&
            maxValue > minValue &&
            increment != 0 &&
            // Math.abs(increment) < maxValue - minValue
            // use BigInteger to avoid overflows when maxValue and minValue
            // are really big
            BigInteger.valueOf(increment).abs().compareTo(
                    BigInteger.valueOf(maxValue).subtract(BigInteger.valueOf(minValue))) < 0;
    }

    private static long getDefaultMinValue(Long startValue, long increment) {
        long v = increment >= 0 ? 1 : Long.MIN_VALUE;
        if (startValue != null && increment >= 0 && startValue < v) {
            v = startValue;
        }
        return v;
    }

    private static long getDefaultMaxValue(Long startValue, long increment) {
        long v = increment >= 0 ? Long.MAX_VALUE : -1;
        if (startValue != null && increment < 0 && startValue > v) {
            v = startValue;
        }
        return v;
    }

    private long getDefaultStartValue(long increment) {
        return increment >= 0 ? minValue : maxValue;
    }

    public boolean getBelongsToTable() {
        return belongsToTable;
    }

    public long getIncrement() {
        return increment;
    }

    public long getMinValue() {
        return minValue;
    }

    public long getMaxValue() {
        return maxValue;
    }

    public boolean getCycle() {
        return cycle;
    }

    public void setCycle(boolean cycle) {
        this.cycle = cycle;
    }


    /**
     * Get the next value for this sequence.
     *
     * @param session the session
     * @return the next value
     */
    public long getNext(Session session) {
        throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED_1,"sequence");
    }

    public long getCurrentValue() {
        throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED_1,"sequence");
    }
    
    @Override
    public int getType() {
        return DbObject.SEQUENCE;
    }

    @Override
    public void checkRename() {
        // nothing to do
    }



    public void setBelongsToTable(boolean b) {
        this.belongsToTable = b;
    }

    public void setCacheSize(long cacheSize) {
        this.cacheSize = Math.max(1, cacheSize);
    }

    public long getCacheSize() {
        return cacheSize;
    }
}
