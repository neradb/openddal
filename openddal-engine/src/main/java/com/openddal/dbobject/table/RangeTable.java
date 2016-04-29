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
package com.openddal.dbobject.table;

import java.util.ArrayList;

import com.openddal.command.expression.Expression;
import com.openddal.dbobject.index.Index;
import com.openddal.dbobject.schema.Schema;
import com.openddal.engine.Session;
import com.openddal.message.DbException;
import com.openddal.value.Value;

/**
 * The table SYSTEM_RANGE is a virtual table that generates incrementing numbers
 * with a given start end end point.
 */
public class RangeTable extends Table {

    /**
     * The name of the range table.
     */
    public static final String NAME = "SYSTEM_RANGE";

    private Expression min, max;
    private boolean optimized;

    /**
     * Create a new range with the given start and end expressions.
     *
     * @param schema the schema (always the main schema)
     * @param min the start expression
     * @param max the end expression
     * @param noColumns whether this table has no columns
     */
    public RangeTable(Schema schema, Expression min, Expression max, boolean noColumns) {
        super(schema, NAME);
        Column[] cols = noColumns ? new Column[0] : new Column[] { new Column("X", Value.LONG) };
        this.min = min;
        this.max = max;
        setColumns(cols);
    }

    @Override
    public String getSQL() {
        return NAME + "(" + min.getSQL() + ", " + max.getSQL() + ")";
    }

    @Override
    public void checkRename() {
        throw DbException.getUnsupportedException("SYSTEM_RANGE");
    }

    @Override
    public String getTableType() {
        throw DbException.throwInternalError();
    }

    /**
     * Calculate and get the start value of this range.
     *
     * @param session the session
     * @return the start value
     */
    public long getMin(Session session) {
        optimize(session);
        return min.getValue(session).getLong();
    }

    /**
     * Calculate and get the end value of this range.
     *
     * @param session the session
     * @return the end value
     */
    public long getMax(Session session) {
        optimize(session);
        return max.getValue(session).getLong();
    }

    private void optimize(Session s) {
        if (!optimized) {
            min = min.optimize(s);
            max = max.optimize(s);
            optimized = true;
        }
    }

    @Override
    public ArrayList<Index> getIndexes() {
        return null;
    }

    @Override
    public Index getUniqueIndex() {
        return null;
    }

    @Override
    public long getRowCountApproximation() {
        return 100;
    }

    @Override
    public boolean isDeterministic() {
        return true;
    }

}
