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

import java.text.Collator;

import com.openddal.command.CommandInterface;
import com.openddal.command.Prepared;
import com.openddal.command.expression.Expression;
import com.openddal.command.expression.ValueExpression;
import com.openddal.dbobject.schema.Schema;
import com.openddal.engine.Database;
import com.openddal.engine.Mode;
import com.openddal.engine.Session;
import com.openddal.message.DbException;
import com.openddal.message.ErrorCode;
import com.openddal.result.ResultInterface;
import com.openddal.value.CompareMode;
import com.openddal.value.ValueInt;

/**
 * This class represents the statement
 * SET
 */
public class Set extends Prepared {

    private final int type;
    private Expression expression;
    private String stringValue;
    private String[] stringValueList;

    public Set(Session session, int type) {
        super(session);
        this.type = type;
    }

    public void setString(String v) {
        this.stringValue = v;
    }

    @Override
    public boolean isTransactional() {
        switch (type) {
        case SetTypes.QUERY_TIMEOUT:
        case SetTypes.SCHEMA:
            return true;
        default:
        }
        return false;
    }

    @Override
    public int update() {
        Database database = session.getDatabase();
        switch (type) {
        case SetTypes.ALLOW_LITERALS: {
            session.getUser().checkAdmin();
            int value = getIntValue();
            if (value < 0 || value > 2) {
                throw DbException.getInvalidValueException("ALLOW_LITERALS", getIntValue());
            }
            database.setAllowLiterals(value);
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
        case SetTypes.MODE:
            Mode mode = Mode.getInstance(stringValue);
            if (mode == null) {
                throw DbException.get(ErrorCode.UNKNOWN_MODE_1, stringValue);
            }
            if (database.getMode() != mode) {
                session.getUser().checkAdmin();
                database.setMode(mode);
            }
            break;
        case SetTypes.QUERY_TIMEOUT: {
            if (getIntValue() < 0) {
                throw DbException.getInvalidValueException("QUERY_TIMEOUT", getIntValue());
            }
            int value = getIntValue();
            session.setQueryTimeout(value);
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
        case SetTypes.SCHEMA: {
            Schema schema = database.getSchema(stringValue);
            session.setCurrentSchema(schema);
            break;
        }
        case SetTypes.SCHEMA_SEARCH_PATH: {
            session.setSchemaSearchPath(stringValueList);
            break;
        }
        case SetTypes.VARIABLE: {
            Expression expr = expression.optimize(session);
            session.setVariable(stringValue, expr.getValue(session));
            break;
        }
        default:
            DbException.throwInternalError("type=" + type);
        }
        return 0;
    }

    private int getIntValue() {
        expression = expression.optimize(session);
        return expression.getValue(session).getInt();
    }

    public void setInt(int value) {
        this.expression = ValueExpression.get(ValueInt.get(value));
    }

    public void setExpression(Expression expression) {
        this.expression = expression;
    }


    @Override
    public boolean needRecompile() {
        return false;
    }

    @Override
    public ResultInterface queryMeta() {
        return null;
    }

    public void setStringArray(String[] list) {
        this.stringValueList = list;
    }

    @Override
    public int getType() {
        return CommandInterface.SET;
    }

}
