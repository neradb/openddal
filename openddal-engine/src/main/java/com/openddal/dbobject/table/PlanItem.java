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

/**
 * The plan item describes the index to be used, and the estimated cost when
 * using it.
 */
public class PlanItem {

    /**
     * The cost.
     */
    double cost;

    private ScanningStrategy strategy = ScanningStrategy.FULL_TABLE_SCAN;
    private PlanItem joinPlan;
    private PlanItem nestedJoinPlan;

    public ScanningStrategy getScanningStrategy() {
        return strategy;
    }

    public void scanningStrategyFor(ScanningStrategy strategy) {
        if(strategy.priority > this.strategy.priority) {
            this.strategy = strategy;
        }
    }
    
    PlanItem getJoinPlan() {
        return joinPlan;
    }

    PlanItem getNestedJoinPlan() {
        return nestedJoinPlan;
    }

    void setJoinPlan(PlanItem joinPlan) {
        this.joinPlan = joinPlan;
    }

    void setNestedJoinPlan(PlanItem nestedJoinPlan) {
        this.nestedJoinPlan = nestedJoinPlan;
    }
    
    enum ScanningStrategy {
        USE_SHARDINGKEY(10), USE_UNIQUEKEY(8), USE_INDEXKEY(6), FULL_TABLE_SCAN(0);
        public final int priority;
        ScanningStrategy(int priority) {
            this.priority = priority;
        }
        
    }

}
