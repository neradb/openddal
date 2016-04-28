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
package com.openddal.excutor.dml;

import java.text.Collator;

import com.openddal.command.dml.Set;
import com.openddal.command.dml.SetTypes;
import com.openddal.command.expression.Expression;
import com.openddal.dbobject.schema.Schema;
import com.openddal.engine.Database;
import com.openddal.excutor.CommonPreparedExecutor;
import com.openddal.message.DbException;
import com.openddal.value.CompareMode;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class SetExecutor extends CommonPreparedExecutor<Set> {

    /**
     * @param prepared
     */
    public SetExecutor(Set prepared) {
        super(prepared);
    }

    @Override
    public int executeUpdate() {
        Database database = session.getDatabase();
        String stringValue = prepared.getStringValue();
        int type = prepared.getSetType();
        switch (type) {
        case SetTypes.QUERY_TIMEOUT: {
            if (getIntValue() < 0) {
                throw DbException.getInvalidValueException("QUERY_TIMEOUT", getIntValue());
            }
            int value = getIntValue();
            session.setQueryTimeout(value);
            break;
        }
        case SetTypes.ALLOW_LITERALS: {
            session.getUser().checkAdmin();
            int value = getIntValue();
            if (value < 0 || value > 2) {
                throw DbException.getInvalidValueException("ALLOW_LITERALS", getIntValue());
            }
            database.setAllowLiterals(value);
            break;
        }
        case SetTypes.IGNORECASE:
            session.getUser().checkAdmin();
            database.setIgnoreCase(getIntValue() == 1);
            break;

        case SetTypes.MAX_MEMORY_ROWS: {
            if (getIntValue() < 0) {
                throw DbException.getInvalidValueException("MAX_MEMORY_ROWS", getIntValue());
            }
            session.getUser().checkAdmin();
            database.setMaxMemoryRows(getIntValue());
            break;
        }
        case SetTypes.MAX_OPERATION_MEMORY: {
            if (getIntValue() < 0) {
                throw DbException.getInvalidValueException("MAX_OPERATION_MEMORY", getIntValue());
            }
            session.getUser().checkAdmin();
            int value = getIntValue();
            database.setMaxOperationMemory(value);
            break;
        }
        case SetTypes.SCHEMA: {
            Schema schema = database.getSchema(stringValue);
            session.setCurrentSchema(schema);
            break;
        }
        case SetTypes.QUERY_STATISTICS: {
            session.getUser().checkAdmin();
            int value = getIntValue();
            if (value < 0 || value > 1) {
                throw DbException.getInvalidValueException("QUERY_STATISTICS", getIntValue());
            }
            database.setQueryStatistics(value == 1);
            break;
        }
        case SetTypes.QUERY_STATISTICS_MAX_ENTRIES: {
            session.getUser().checkAdmin();
            int value = getIntValue();
            if (value < 1) {
                throw DbException.getInvalidValueException("QUERY_STATISTICS_MAX_ENTRIES", getIntValue());
            }
            database.setQueryStatisticsMaxEntries(value);
            break;
        }
        case SetTypes.COLLATION: {
            session.getUser().checkAdmin();
            final boolean binaryUnsigned = database.getCompareMode().isBinaryUnsigned();
            CompareMode compareMode;
            StringBuilder buff = new StringBuilder(stringValue);
            if (stringValue.equals(CompareMode.OFF)) {
                compareMode = CompareMode.getInstance(null, 0, binaryUnsigned);
            } else {
                int strength = getIntValue();
                buff.append(" STRENGTH ");
                if (strength == Collator.IDENTICAL) {
                    buff.append("IDENTICAL");
                } else if (strength == Collator.PRIMARY) {
                    buff.append("PRIMARY");
                } else if (strength == Collator.SECONDARY) {
                    buff.append("SECONDARY");
                } else if (strength == Collator.TERTIARY) {
                    buff.append("TERTIARY");
                }
                compareMode = CompareMode.getInstance(stringValue, strength, binaryUnsigned);
            }
            CompareMode old = database.getCompareMode();
            if (old.equals(compareMode)) {
                break;
            }
            database.setCompareMode(compareMode);
            break;
        }
        case SetTypes.BINARY_COLLATION: {
            session.getUser().checkAdmin();
            CompareMode currentMode = database.getCompareMode();
            CompareMode newMode;
            if (stringValue.equals(CompareMode.SIGNED)) {
                newMode = CompareMode.getInstance(currentMode.getName(), currentMode.getStrength(), false);
            } else if (stringValue.equals(CompareMode.UNSIGNED)) {
                newMode = CompareMode.getInstance(currentMode.getName(), currentMode.getStrength(), true);
            } else {
                throw DbException.getInvalidValueException("BINARY_COLLATION", stringValue);
            }
            database.setCompareMode(newMode);
            break;
        }
        default:
            DbException.throwInternalError("type=" + type);
        }
        return 0;
    }

    private int getIntValue() {
        Expression expression = prepared.getExpression();
        expression = expression.optimize(session);
        return expression.getValue(session).getInt();
    }

}
