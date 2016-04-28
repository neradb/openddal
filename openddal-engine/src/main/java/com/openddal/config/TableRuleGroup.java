package com.openddal.config;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class TableRuleGroup extends ShardedTableRule implements Serializable {

    private static final long serialVersionUID = 1L;
    

    private List<TableRule> tableRules;

    public List<TableRule> getTableRules() {
        for (TableRule tableRule : tableRules) {
            tableRule.setOwnerGroup(this);
            if(tableRule instanceof ShardedTableRule) {
                List<String> ruleColumns = this.getRuleColumns();
                
            } else if(tableRule instanceof MultiNodeTableRule) {
                
            } else {
                
            }
        }
        return tableRules;
    }

    public void setTableRules(List<TableRule> tableRules) {
        this.tableRules = tableRules;
    }
    
    public void setTableRules(TableRule ... tableRules) {
        this.tableRules = Arrays.asList(tableRules);
    }

}
