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
import java.util.List;

import com.openddal.config.ShardedTableRule;
import com.openddal.config.TableRule;
import com.openddal.dbobject.index.IndexCondition;
import com.openddal.util.New;

/**
 * The plan item describes the index to be used, and the estimated cost when
 * using it.
 */
public class PlanItem {

    /**
     * The cost.
     */
    double cost;

    private List<TableFilter> tableFilters = New.arrayList();
    private PlanItem joinPlan;
    private PlanItem nestedJoinPlan;

    PlanItem getJoinPlan() {
        return joinPlan;
    }

    void setJoinPlan(PlanItem joinPlan) {
        this.joinPlan = joinPlan;
    }

    PlanItem getNestedJoinPlan() {
        return nestedJoinPlan;
    }

    void setNestedJoinPlan(PlanItem nestedJoinPlan) {
        this.nestedJoinPlan = nestedJoinPlan;
    }

    void addTableFilter(TableFilter filter) {
        tableFilters.add(filter);
    }

    boolean addJoinFilter(TableFilter filter) {
        if (!filter.isFromTableMate()) {
            return false;
        }
        TableMate table1 = (TableMate) filter.getTable();
        for (TableFilter item : tableFilters) {
            if (!item.isFromTableMate()) {
                return false;
            }
            TableMate table2 = (TableMate) item.getTable();
            TableRule rule1 = table1.getTableRule();
            TableRule rule2 = table2.getTableRule();
            if (!rule1.isNodeComparable(rule2)) {
                return false;
            }
        }
        if (table1.getTableRule().getClass() == ShardedTableRule.class) {
            ArrayList<IndexCondition> conditions = filter.getIndexConditions();
            Column[] lefts = table1.getRuleColumns();
            for (TableFilter item : tableFilters) {
                TableMate table2 = (TableMate) item.getTable();
                if (table2.getTableRule().getClass() != ShardedTableRule.class) {
                    continue;
                }
                Column[] rights = table2.getRuleColumns();
                for (int i = 0; i < lefts.length && lefts.length == rights.length; i++) {
                    boolean columnExisting = false;
                    for (IndexCondition condition : conditions) {
                        columnExisting = condition.isColumnEquality(lefts[i], rights[i]);
                        if (columnExisting) {
                            break;
                        }
                    }
                    if (!columnExisting) {
                        return false;
                    }
                }
            }
        }
        tableFilters.add(filter);
        return true;
    }

}
