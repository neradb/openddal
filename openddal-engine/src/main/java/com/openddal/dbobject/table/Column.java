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

import java.sql.ResultSetMetaData;

import com.openddal.command.Parser;
import com.openddal.command.expression.ConditionAndOr;
import com.openddal.command.expression.Expression;
import com.openddal.command.expression.ExpressionVisitor;
import com.openddal.command.expression.ValueExpression;
import com.openddal.dbobject.schema.Sequence;
import com.openddal.engine.Constants;
import com.openddal.engine.Session;
import com.openddal.message.DbException;
import com.openddal.message.ErrorCode;
import com.openddal.result.Row;
import com.openddal.util.MathUtils;
import com.openddal.util.StringUtils;
import com.openddal.value.DataType;
import com.openddal.value.Value;
import com.openddal.value.ValueNull;

/**
 * This class represents a column in a table.
 */
public class Column {

    /**
     * The name of the rowid pseudo column.
     */
    public static final String ROWID = "_ROWID_";

    /**
     * This column is not nullable.
     */
    public static final int NOT_NULLABLE =
            ResultSetMetaData.columnNoNulls;

    /**
     * This column is nullable.
     */
    public static final int NULLABLE =
            ResultSetMetaData.columnNullable;

    /**
     * It is not know whether this column is nullable.
     */
    public static final int NULLABLE_UNKNOWN =
            ResultSetMetaData.columnNullableUnknown;

    private final int type;
    private long precision;
    private int scale;
    private int displaySize;
    private Table table;
    private String name;
    private int columnId;
    private boolean nullable = true;
    private Expression defaultExpression;
    private Expression checkConstraint;
    private String checkConstraintSQL;
    private String originalSQL;
    private boolean autoIncrement;
    private boolean convertNullToDefault;
    private Sequence sequence;
    private boolean isComputed;
    private TableFilter computeTableFilter;
    private int selectivity;
    private SingleColumnResolver resolver;
    private String comment;
    private boolean primaryKey;

    public Column(String name, int type) {
        this(name, type, -1, -1, -1);
    }

    public Column(String name, int type, long precision, int scale,
                  int displaySize) {
        this.name = name;
        this.type = type;
        if (precision == -1 && scale == -1 && displaySize == -1) {
            DataType dt = DataType.getDataType(type);
            precision = dt.defaultPrecision;
            scale = dt.defaultScale;
            displaySize = dt.defaultDisplaySize;
        }
        this.precision = precision;
        this.scale = scale;
        this.displaySize = displaySize;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        } else if (!(o instanceof Column)) {
            return false;
        }
        Column other = (Column) o;
        if (table == null || other.table == null ||
                name == null || other.name == null) {
            return false;
        }
        if (table != other.table) {
            return false;
        }
        return name.equals(other.name);
    }

    @Override
    public int hashCode() {
        if (table == null || name == null) {
            return 0;
        }
        return table.getId() ^ name.hashCode();
    }

    public Column getClone() {
        Column newColumn = new Column(name, type, precision, scale, displaySize);
        newColumn.copy(this);
        return newColumn;
    }

    /**
     * Convert a value to this column's type.
     *
     * @param v the value
     * @return the value
     */
    public Value convert(Value v) {
        try {
            return v.convertTo(type);
        } catch (DbException e) {
            if (e.getErrorCode() == ErrorCode.DATA_CONVERSION_ERROR_1) {
                String target = (table == null ? "" : table.getName() + ": ") +
                        getCreateSQL();
                throw DbException.get(
                        ErrorCode.DATA_CONVERSION_ERROR_1,
                        v.getSQL() + " (" + target + ")");
            }
            throw e;
        }
    }

    boolean getComputed() {
        return isComputed;
    }

    /**
     * Compute the value of this computed column.
     *
     * @param session the session
     * @param row     the row
     * @return the value
     */
    synchronized Value computeValue(Session session, Row row) {
        computeTableFilter.setSession(session);
        computeTableFilter.set(row);
        return defaultExpression.getValue(session);
    }

    /**
     * Set the default value in the form of a computed expression of other
     * columns.
     *
     * @param expression the computed expression
     */
    public void setComputedExpression(Expression expression) {
        this.isComputed = true;
        this.defaultExpression = expression;
    }

    /**
     * Set the table and column id.
     *
     * @param table    the table
     * @param columnId the column index
     */
    public void setTable(Table table, int columnId) {
        this.table = table;
        this.columnId = columnId;
    }

    public Table getTable() {
        return table;
    }

    /**
     * Set the default expression.
     *
     * @param session           the session
     * @param defaultExpression the default expression
     */
    public void setDefaultExpression(Session session,
                                     Expression defaultExpression) {
        // also to test that no column names are used
        if (defaultExpression != null) {
            defaultExpression = defaultExpression.optimize(session);
            if (defaultExpression.isConstant()) {
                defaultExpression = ValueExpression.get(
                        defaultExpression.getValue(session));
            }
        }
        this.defaultExpression = defaultExpression;
    }

    public int getColumnId() {
        return columnId;
    }

    public String getSQL() {
        return Parser.quoteIdentifier(name);
    }

    public String getName() {
        return name;
    }

    public int getType() {
        return type;
    }

    public long getPrecision() {
        return precision;
    }

    public void setPrecision(long p) {
        precision = p;
    }

    public int getDisplaySize() {
        return displaySize;
    }

    public int getScale() {
        return scale;
    }

    /**
     * Prepare all expressions of this column.
     *
     * @param session the session
     */
    public void prepareExpression(Session session) {
        if (defaultExpression != null) {
            computeTableFilter = new TableFilter(session, table, null, false, null);
            defaultExpression.mapColumns(computeTableFilter, 0);
            defaultExpression = defaultExpression.optimize(session);
        }
    }

    public String getCreateSQL() {
        StringBuilder buff = new StringBuilder();
        if (name != null) {
            buff.append(Parser.quoteIdentifier(name)).append(' ');
        }
        if (originalSQL != null) {
            buff.append(originalSQL);
        } else {
            buff.append(DataType.getDataType(type).name);
            switch (type) {
                case Value.DECIMAL:
                    buff.append('(').append(precision).append(", ").append(scale).append(')');
                    break;
                case Value.BYTES:
                case Value.STRING:
                case Value.STRING_IGNORECASE:
                case Value.STRING_FIXED:
                    if (precision < Integer.MAX_VALUE) {
                        buff.append('(').append(precision).append(')');
                    }
                    break;
                default:
            }
        }
        if (defaultExpression != null) {
            String sql = defaultExpression.getSQL();
            if (sql != null) {
                if (isComputed) {
                    buff.append(" AS ").append(sql);
                } else if (defaultExpression != null) {
                    buff.append(" DEFAULT ").append(sql);
                }
            }
        }
        if (!nullable) {
            buff.append(" NOT NULL");
        }
        if (convertNullToDefault) {
            buff.append(" NULL_TO_DEFAULT");
        }
        if (sequence != null) {
            buff.append(" SEQUENCE ").append(sequence.getSQL());
        }
        if (selectivity != 0) {
            buff.append(" SELECTIVITY ").append(selectivity);
        }
        if (comment != null) {
            buff.append(" COMMENT ").append(StringUtils.quoteStringSQL(comment));
        }
        if (checkConstraint != null) {
            buff.append(" CHECK ").append(checkConstraintSQL);
        }
        return buff.toString();
    }

    public boolean isNullable() {
        return nullable;
    }

    public void setNullable(boolean b) {
        nullable = b;
    }

    public String getOriginalSQL() {
        return originalSQL;
    }

    public void setOriginalSQL(String original) {
        originalSQL = original;
    }

    public Expression getDefaultExpression() {
        return defaultExpression;
    }

    public boolean isAutoIncrement() {
        return autoIncrement;
    }

    /**
     * Set the autoincrement flag and related properties of this column.
     *
     * @param autoInc   the new autoincrement flag
     * @param start     the sequence start value
     * @param increment the sequence increment
     */
    public void setAutoIncrement(boolean autoInc, long start, long increment) {
        this.autoIncrement = autoInc;
        this.nullable = false;
        if (autoInc) {
            convertNullToDefault = true;
        }
    }

    public void setConvertNullToDefault(boolean convert) {
        this.convertNullToDefault = convert;
    }

    /**
     * Rename the column. This method will only set the column name to the new
     * value.
     *
     * @param newName the new column name
     */
    public void rename(String newName) {
        this.name = newName;
    }

    public Sequence getSequence() {
        return sequence;
    }

    public void setSequence(Sequence sequence) {
        this.sequence = sequence;
    }

    /**
     * Get the selectivity of the column. Selectivity 100 means values are
     * unique, 10 means every distinct value appears 10 times on average.
     *
     * @return the selectivity
     */
    public int getSelectivity() {
        return selectivity == 0 ? Constants.SELECTIVITY_DEFAULT : selectivity;
    }

    /**
     * Set the new selectivity of a column.
     *
     * @param selectivity the new value
     */
    public void setSelectivity(int selectivity) {
        selectivity = selectivity < 0 ? 0 : (selectivity > 100 ? 100 : selectivity);
        this.selectivity = selectivity;
    }

    /**
     * Add a check constraint expression to this column. An existing check
     * constraint constraint is added using AND.
     *
     * @param session the session
     * @param expr    the (additional) constraint
     */
    public void addCheckConstraint(Session session, Expression expr) {
        if (expr == null) {
            return;
        }
        resolver = new SingleColumnResolver(this);
        synchronized (this) {
            String oldName = name;
            if (name == null) {
                name = "VALUE";
            }
            expr.mapColumns(resolver, 0);
            name = oldName;
        }
        expr = expr.optimize(session);
        resolver.setValue(ValueNull.INSTANCE);
        // check if the column is mapped
        synchronized (this) {
            expr.getValue(session);
        }
        if (checkConstraint == null) {
            checkConstraint = expr;
        } else {
            checkConstraint = new ConditionAndOr(ConditionAndOr.AND, checkConstraint, expr);
        }
        checkConstraintSQL = getCheckConstraintSQL(session, name);
    }

    /**
     * Remove the check constraint if there is one.
     */
    public void removeCheckConstraint() {
        checkConstraint = null;
        checkConstraintSQL = null;
    }

    /**
     * Get the check constraint expression for this column if set.
     *
     * @param session      the session
     * @param asColumnName the column name to use
     * @return the constraint expression
     */
    public Expression getCheckConstraint(Session session, String asColumnName) {
        if (checkConstraint == null) {
            return null;
        }
        Parser parser = new Parser(session);
        String sql;
        synchronized (this) {
            String oldName = name;
            name = asColumnName;
            sql = checkConstraint.getSQL();
            name = oldName;
        }
        Expression expr = parser.parseExpression(sql);
        return expr;
    }

    String getDefaultSQL() {
        return defaultExpression == null ? null : defaultExpression.getSQL();
    }

    int getPrecisionAsInt() {
        return MathUtils.convertLongToInt(precision);
    }

    DataType getDataType() {
        return DataType.getDataType(type);
    }

    /**
     * Get the check constraint SQL snippet.
     *
     * @param session      the session
     * @param asColumnName the column name to use
     * @return the SQL snippet
     */
    String getCheckConstraintSQL(Session session, String asColumnName) {
        Expression constraint = getCheckConstraint(session, asColumnName);
        return constraint == null ? "" : constraint.getSQL();
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    /**
     * Visit the default expression, the check constraint, and the sequence (if
     * any).
     *
     * @param visitor the visitor
     * @return true if every visited expression returned true, or if there are
     * no expressions
     */
    boolean isEverything(ExpressionVisitor visitor) {
        if (visitor.getType() == ExpressionVisitor.GET_DEPENDENCIES) {
            if (sequence != null) {
                visitor.getDependencies().add(sequence);
            }
        }
        if (defaultExpression != null && !defaultExpression.isEverything(visitor)) {
            return false;
        }
        return !(checkConstraint != null && !checkConstraint.isEverything(visitor));
    }

    public boolean isPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(boolean primaryKey) {
        this.primaryKey = primaryKey;
    }

    @Override
    public String toString() {
        return name;
    }

    /**
     * Copy the data of the source column into the current column.
     *
     * @param source the source column
     */
    public void copy(Column source) {
        checkConstraint = source.checkConstraint;
        checkConstraintSQL = source.checkConstraintSQL;
        displaySize = source.displaySize;
        name = source.name;
        precision = source.precision;
        scale = source.scale;
        // table is not set
        // columnId is not set
        nullable = source.nullable;
        defaultExpression = source.defaultExpression;
        originalSQL = source.originalSQL;
        // autoIncrement, start, increment is not set
        convertNullToDefault = source.convertNullToDefault;
        sequence = source.sequence;
        comment = source.comment;
        computeTableFilter = source.computeTableFilter;
        isComputed = source.isComputed;
        selectivity = source.selectivity;
        primaryKey = source.primaryKey;
    }

}
