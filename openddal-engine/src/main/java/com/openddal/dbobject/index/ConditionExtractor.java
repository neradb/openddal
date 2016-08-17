package com.openddal.dbobject.index;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

import com.openddal.command.expression.Comparison;
import com.openddal.dbobject.table.Column;
import com.openddal.dbobject.table.Table;
import com.openddal.dbobject.table.TableFilter;
import com.openddal.engine.Database;
import com.openddal.engine.Session;
import com.openddal.result.ResultInterface;
import com.openddal.result.SearchRow;
import com.openddal.util.New;
import com.openddal.value.Value;
import com.openddal.value.ValueNull;

/**
 * @author jorgie.li
 */
public final class ConditionExtractor {

    private Session session;
    private Table table;
    private ArrayList<IndexCondition> indexConditions;
    private boolean alwaysFalse;

    private SearchRow start, end;
    private Map<Column, Set<Value>> inColumns = New.hashMap();

    public ConditionExtractor(TableFilter filter) {
        this.session = filter.getSession();
        this.table = filter.getTable();
        this.indexConditions = filter.getIndexConditions();
        this.doExtract();
    }

    /**
     * Prepare this index cursor to make a lookup in index.
     *
     * @param s Session.
     * @param indexConditions Index conditions.
     */
    private void doExtract() {
        alwaysFalse = false;
        start = end = null;
        Database db = session.getDatabase();
        // don't use enhanced for loop to avoid creating objects
        for (int i = 0, size = indexConditions.size(); i < size; i++) {
            IndexCondition condition = indexConditions.get(i);
            if (!condition.isEvaluatable()) {
                continue;
            }
            if (condition.isAlwaysFalse()) {
                alwaysFalse = true;
                break;
            }
            Column column = condition.getColumn();
            if (condition.getCompareType() == Comparison.IN_LIST) {
                Value[] inList = condition.getCurrentValueList(session);
                Set<Value> values = inColumns.get(column);
                if (values == null) {
                    values = New.hashSet();
                    inColumns.put(column, values);
                }
                for (Value v : inList) {
                    if (v != ValueNull.INSTANCE) {
                        values.add(v);
                    }
                }

            } else if (condition.getCompareType() == Comparison.IN_QUERY) {
                ResultInterface inResult = condition.getCurrentResult();
                Set<Value> values = inColumns.get(column);
                if (values == null) {
                    values = New.hashSet();
                    inColumns.put(column, values);
                }
                while (inResult.next()) {
                    Value v = inResult.currentRow()[0];
                    if (v != ValueNull.INSTANCE) {
                        v = column.convert(v);
                        values.add(v);
                    }
                }
            } else {
                Value v = condition.getCurrentValue(session);
                boolean isStart = condition.isStart();
                boolean isEnd = condition.isEnd();
                int columnId = column.getColumnId();
                if (isStart) {
                    start = getSearchRow(start, columnId, v, true);
                }
                if (isEnd) {
                    end = getSearchRow(end, columnId, v, false);
                }
                if (!db.getSettings().optimizeIsNull) {
                    if (isStart && isEnd) {
                        if (v == ValueNull.INSTANCE) {
                            // join on a column=NULL is always false
                            alwaysFalse = true;
                        }
                    }
                }
            }
            if(alwaysFalse) {
                return;
            }
        }
        
        
        for (int i = 0, size = indexConditions.size(); i < size; i++) {
            IndexCondition condition = indexConditions.get(i);
            Column column = condition.getColumn();
            int idx = column.getColumnId();
            Value v1 = start == null ? null : start.getValue(idx);
            Value v2 = end == null ? null : end.getValue(idx);
            if (v1 == null && v2 == null) {
                continue;
            } else if (v1 != null || v2 != null) {
                if (v1 != null) {
                    //把小于v1的值都去掉
                    Set<Value> values = inColumns.get(column);
                    if (values != null) {
                        for (Value value : values) {
                            if (db.compare(value, v1) < 0) {
                                values.remove(value);
                            }
                        }
                        if (values.isEmpty()) {
                            alwaysFalse = true;
                        }
                    }
                } else {
                    //把大于v2的值都去掉
                    Set<Value> values = inColumns.get(column);
                    if (values != null) {
                        for (Value value : values) {
                            if (db.compare(value, v1) > 0) {
                                values.remove(value);
                            }
                        }
                        if (values.isEmpty()) {
                            alwaysFalse = true;
                        }
                    }
                }
            } else {
                // v1 < x < v2, v1 > v2
                if (db.compare(v1, v2) > 0) {
                    alwaysFalse = true;
                }
            }
        }
        if(alwaysFalse) {
            return;
        }

    }

    private SearchRow getSearchRow(SearchRow row, int columnId, Value v, boolean max) {
        if (row == null) {
            row = table.getTemplateRow();
        } else {
            v = getMax(row.getValue(columnId), v, max);
        }
        if (columnId < 0) {
            row.setKey(v.getLong());
        } else {
            row.setValue(columnId, v);
        }
        return row;
    }

    private Value getMax(Value a, Value b, boolean bigger) {
        if (a == null) {
            return b;
        } else if (b == null) {
            return a;
        }
        if (session.getDatabase().getSettings().optimizeIsNull) {
            // IS NULL must be checked later
            if (a == ValueNull.INSTANCE) {
                return b;
            } else if (b == ValueNull.INSTANCE) {
                return a;
            }
        }
        int comp = a.compareTo(b, table.getDatabase().getCompareMode());
        if (comp == 0) {
            return a;
        }
        if (a == ValueNull.INSTANCE || b == ValueNull.INSTANCE) {
            if (session.getDatabase().getSettings().optimizeIsNull) {
                // column IS NULL AND column <op> <not null> is always false
                return null;
            }
        }
        if (!bigger) {
            comp = -comp;
        }
        return comp > 0 ? a : b;
    }

    /**
     * Check if the result is empty for sure.
     *
     * @return true if it is
     */
    public boolean isAlwaysFalse() {
        return alwaysFalse;
    }

    /**
     * Get start search row.
     *
     * @return search row
     */
    public SearchRow getStart() {
        return start;
    }

    /**
     * Get end search row.
     *
     * @return search row
     */
    public SearchRow getEnd() {
        return end;
    }

    public Map<Column, Set<Value>> getInColumns() {
        return inColumns;
    }

}
